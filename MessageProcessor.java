package axa.partners.clp.mrtdocumentbridge.controller;

import axa.partners.clp.mrtdocumentbridge.data.Document;
import axa.partners.clp.mrtdocumentbridge.data.Request;
import axa.partners.clp.mrtdocumentbridge.exception.ServiceException;
import axa.partners.clp.mrtdocumentbridge.service.ClientService;
import axa.partners.clp.mrtdocumentbridge.service.FileHandlingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import static axa.partners.clp.mrtdocumentbridge.config.RabbitMQConfiguration.*;

@Component
public class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    static final String HEADER_X_RETRIES_COUNT = "X-Retries-Count";

    public static final String HEADER_X_ORIGINAL_EXCHANGE = "X-Original-Exchange";

    public static final String HEADER_X_ORIGINAL_ROUTING_KEY = "X-Original-Routing-Key";

    @Value("${rabbitmq.maxRetries:5}")
    int maxRetries;

    @Value("${rabbitmq.dlq.initalDelay:2}")
    int initialDelay;

    @Value("${rabbitmq.dlq.multiplier:2}")
    int multiplier;

    private final AmqpAdmin amqpAdmin;

    private final AmqpTemplate amqpTemplate;

    private final ClientService clientService;

    private final String requestPath;

    private final OpenTelemetry openTelemetry;

    private final Tracer tracer;

    private final ObjectMapper objectMapper;

    /**
     * Message processor default constructor. Attempts to autowire the dependencies
     *
     * @param amqpTemplate amqp template to access AMQP features
     * @param clientService service that interacts with DB
     */
    @Autowired
    public MessageProcessor(AmqpAdmin amqpAdmin,
                            AmqpTemplate amqpTemplate,
                            ClientService clientService,
                            FileHandlingService fileHandlingService,
                            OpenTelemetry openTelemetry,
                            String version,
                            ObjectMapper objectMapper) {
        this.amqpAdmin = amqpAdmin;
        this.amqpTemplate = amqpTemplate;
        this.clientService = clientService;
        this.requestPath = fileHandlingService.path("request");
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer(MessageProcessor.class.getName(), version);
        this.objectMapper = objectMapper;
    }

    /**
     * Document Creation/Registration queue listener
     *
     * @param message inbound message
     */
    @RabbitListener(
            id = "process",
            queues = AMQP_REQUEST_QUEUE)
    @Transactional
    public void process(Message message) {
        Span serverSpan = buildSpan(message.getMessageProperties());

        try (Scope scope = serverSpan.makeCurrent()) {
            Request request = null;
            try {
                request = this.objectMapper.readValue(message.getBody(), Request.class);
            } catch (Throwable e) {
                throw new AmqpRejectAndDontRequeueException(String.format("Cannot convert message payload: %s'", e.getMessage()));
            }

            if (request.getCorrelationId() != null) {
                serverSpan.setAttribute("correlationId", request.getCorrelationId());
                logger.info("Processing message (correlationId '{}')", request.getCorrelationId());
            }

            if (request.getBatchId() != null && !request.getBatchId().isEmpty()) {
                serverSpan.setAttribute("batchId", request.getBatchId());
                logger.info("Processing batch '{}'", request.getBatchId());
            }
            if (request.getDocuments() != null) {
                serverSpan.setAttribute("numberOfDocument", request.getDocuments().size());
                logger.info("Received total {} requests", request.getDocuments().size());
            }

            int i = 0;
            for (Document document : request.getDocuments()) {
                serverSpan.setAttribute(String.format("request_id_%d", i++), document.getRequestId());
                logger.info("Processing request '{}'", document.getRequestId());
            }

            if (!this.validateCommand(request.getCommand())) {
                throw new AmqpRejectAndDontRequeueException("Incorrect command in the message: '" + request.getCommand() + "'");
            }

            serverSpan.setAttribute("command", request.getCommand());

            try {
                if (!this.clientService.checkIfClientRequestExists(request)) {
                    this.clientService.addClientRequest(request, "doc_registered");

                    if (request.getCommand().equals("doc_create")) {
                        File outputFile = new File(new File(this.requestPath), String.format("GenworthMRBatch_%d.xml", request.getDocuments().get(0).getFileId()));
                        this.storePayload(outputFile, request.getPayload());
                    }
                } else {
                    logger.error("Client request does already exist. Ignoring...");
                }
            } catch (ServiceException e) {
                // TODO delete created request
                throw new AmqpRejectAndDontRequeueException(e.getMessage());
            }
        } finally {
            serverSpan.end();
        }
    }

    private boolean validateCommand(String command) {
        Span span = this.tracer.spanBuilder("addDocument")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            if (command != null && !command.isEmpty()) {
                if (command.equals("doc_create") || command.equals("doc_register")) {
                    span.setStatus(StatusCode.OK);
                    return true;
                }
            }
            span.setStatus(StatusCode.OK);
            return false;
        } finally {
            span.end();
        }
    }

    @RabbitListener(
            id = "dlq",
            queues = AMQP_DLQ
            )
    @Transactional
    public void dlqProcessor(Message message) {
        Span span = buildSpan(message.getMessageProperties());

        try (Scope scope = span.makeCurrent()) {
            MessageBuilder builder = MessageBuilder.fromMessage(message);
            RetryDetails retryDetails = new RetryDetails(message);

            if (!retryDetails.valid) {
                this.sendToParkingLot(builder);
                return;
            }

            if (retryDetails.retries < maxRetries) {
                builder.setHeader("x-death-count", retryDetails.retries + 1);
                long delay = (this.initialDelay + (this.initialDelay * this.multiplier * (retryDetails.retries - 1))) * 1000L;
                String queueName = String.format("%s-%s-%d-delay", retryDetails.originalExchange, retryDetails.originalRoutingKey, delay);
                this.declareDelayQueue(queueName, retryDetails.originalExchange, retryDetails.originalRoutingKey, delay);
                logger.info("Resending message to temporary TTL queue '{}'", queueName);
                logger.debug("Message body {}", new String(message.getBody()));
                this.amqpTemplate.convertAndSend("", queueName, builder.build());
            } else {
                this.sendToParkingLot(builder);
            }
        } finally {
            span.end();
        }
    }

    void declareDelayQueue(String queueName,
                           String originalExchange,
                           String originalRoutingKey,
                           long ttl) {
        this.amqpAdmin.declareQueue(QueueBuilder
                .durable(queueName)
                .autoDelete()
                .withArgument("x-message-ttl", ttl)
                .withArgument("x-dead-letter-exchange", originalExchange)
                .withArgument("x-dead-letter-routing-key", originalRoutingKey)
                .build());
    }

    @RabbitListener(
            id = "parkingLotProcessor",
            queues = AMQP_PARKING_LOT)
    @Transactional
    public void parkingLotProcessor(Message message) {
        RetryDetails retryDetails = new RetryDetails(message);
        if (!retryDetails.valid) {
            logger.error("Cannot find original message details. Sending to orphan exchange");
            this.amqpTemplate.convertAndSend(AMQP_ORPHAN_EXCHANGE, "", message);
        }

        try {
            this.clientService.addMessageToParkingStore(message, retryDetails.originalExchange, retryDetails.originalRoutingKey);
        } catch (ServiceException e) {
            logger.error("Cannot store parking lot message. Message getting deleted", e);
            this.amqpTemplate.convertAndSend(AMQP_ORPHAN_EXCHANGE, "", message);
        }
    }

    void sendToParkingLot(MessageBuilder builder) {
        builder.removeHeader(HEADER_X_RETRIES_COUNT);
        Message payload = builder.build();
        logger.info("Sending message for '{}' to a parking lot", payload);
        this.amqpTemplate.convertAndSend(AMQP_DEAD_LETTER_EXCHANGE, AMQP_PARKING_LOT, payload);
    }

    private void storePayload(File outputFile, String payload) throws ServiceException {
        Span span = this.tracer
                .spanBuilder("storePayload")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("outputFile", outputFile.getAbsolutePath());
            span.setAttribute("payload", payload);
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile))) {
                bufferedWriter.write(payload);
            } catch (Exception e) {
                throw new ServiceException(String.format("Cannot write output file '%s': %s", outputFile.getAbsolutePath(), e.getMessage()));
            }
            finally {
                span.end();
            }
        }
    }

    private static class RetryDetails {
        long retries;
        String originalExchange;

        String originalRoutingKey;

        boolean valid;

        RetryDetails(Message message) {
            Object xDeathObject = message.getMessageProperties().getHeaders().get("x-death");
            if (xDeathObject == null) {
                valid = false;
                return;
            }

            List<Map<String, Object>> xDeath = (List)xDeathObject;
            if (xDeath.size() == 0) {
                valid = false;
                return;
            }

            Map<String, Object> value = xDeath.get(0);
            retries = (Long)value.get("count");
            originalExchange = (String)value.get("exchange");
            List<String> routingKeys = (List)value.get("routing-keys");
            if (originalExchange == null || routingKeys == null || routingKeys.size() == 0) {
                valid = false;
                return;
            }
            originalRoutingKey = routingKeys.get(0);
            valid = true;
        }
    }

    private Context extractContext(MessageProperties messageProperties) {
        return this.openTelemetry
                .getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), messageProperties.getHeaders(), new TextMapGetter<Map<String, Object>>() {
                    @Override
                    public Iterable<String> keys(Map<String, Object> carrier) {
                        return carrier.keySet();
                    }

                    @Override
                    public String get(Map<String, Object> carrier, String key) {
                        return (String)carrier.get(key);
                    }
                });
    }

    private Span buildSpan(MessageProperties messageProperties) {
        return this.tracer.spanBuilder(messageProperties.getConsumerQueue() + "_receive")
                .setParent(extractContext(messageProperties))
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
    }
}

