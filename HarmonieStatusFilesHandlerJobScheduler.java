package axa.partners.clp.mrtdocumentbridge.task;

import axa.partners.clp.mrtdocumentbridge.data.ClientRequestAndRoutingDetails;
import axa.partners.clp.mrtdocumentbridge.data.Response;
import axa.partners.clp.mrtdocumentbridge.data.ResponseWrapper;
import axa.partners.clp.mrtdocumentbridge.data.cmx.AddDocumentResponse;
import axa.partners.clp.mrtdocumentbridge.data.cmx.Metadata;
import axa.partners.clp.mrtdocumentbridge.data.harmonie.Request;
import axa.partners.clp.mrtdocumentbridge.data.harmonie.StatusDocument;
import axa.partners.clp.mrtdocumentbridge.exception.ServiceException;
import axa.partners.clp.mrtdocumentbridge.processor.ResponseBuilder;
import axa.partners.clp.mrtdocumentbridge.service.CMXClientService;
import axa.partners.clp.mrtdocumentbridge.service.ClientService;
import axa.partners.clp.mrtdocumentbridge.service.FileHandlingService;
import axa.partners.clp.mrtdocumentbridge.service.ProcessPrintedDocuments;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Status file scheduler. Periodically executed to process Harmonie status files for the registered clients
 */
@Component
public class HarmonieStatusFilesHandlerJobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(HarmonieStatusFilesHandlerJobScheduler.class);

    private final CMXClientService cmxClientService;

    private final ClientService clientService;

    private final AmqpTemplate amqpTemplate;

    private final OpenTelemetry openTelemetry;

    private final ObjectMapper objectMapper;

    private final FileHandlingService fileHandlingService;

    private final String inputPath;

    private final String errorPath;

    private final String documentPath;

    private final String processedPath;

    private final String binPath;

    private LocalDate today;

    private final MeterRegistry registry;

    private final Map<String, Timer> timers;

    private final Tracer tracer;

    private final int maxAttempts;

    /**
     * Default constructor
     *
     * @param clientService Database service dealing with the client data
     * @param openTelemetry OpenTelemetry object
     * @param version Application version
     * @param objectMapper object mapper
     */
    public HarmonieStatusFilesHandlerJobScheduler(CMXClientService cmxClientService,
                                                  ClientService clientService,
                                                  AmqpTemplate amqpTemplate,
                                                  FileHandlingService fileHandlingService,
                                                  MeterRegistry registry,
                                                  OpenTelemetry openTelemetry,
                                                  String version,
                                                  @Value("${printed_document.max_attempts:7}") int maxAttempts,
                                                  ObjectMapper objectMapper) throws ServiceException {
        logger.info("Creating HarmonieStatusFilesHandlerJobScheduler");
        this.maxAttempts = maxAttempts;
        logger.info("The printed documents processing threshold is {}", this.maxAttempts);
        this.cmxClientService = cmxClientService;
        this.clientService = clientService;
        this.amqpTemplate = amqpTemplate;
        this.fileHandlingService = fileHandlingService;
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer(HarmonieStatusFilesHandlerJobScheduler.class.getName(), version);
        this.objectMapper = objectMapper;
        this.registry = registry;

        this.timers = new HashMap<>();

        this.inputPath = this.fileHandlingService.path("input");
        this.fileHandlingService.validatePath("input", this.inputPath);

        this.documentPath = this.fileHandlingService.path("document");
        this.fileHandlingService.validatePath("document", this.documentPath);

        this.processedPath = this.fileHandlingService.path("processed");
        this.fileHandlingService.validatePath("processed", this.processedPath);

        this.errorPath = this.fileHandlingService.path("error");
        this.fileHandlingService.validatePath("error", this.errorPath);

        this.binPath = this.fileHandlingService.path("bin");
        this.fileHandlingService.validatePath("bin", this.binPath);

        String requestPath = this.fileHandlingService.path("request");
        this.fileHandlingService.validatePath("request", requestPath);
    }

    /**
     * Job processing method
     */
    @Scheduled(cron = "${cron.harmonieStatusFilesHandlerJobSchedule:0 * * * * *}")
    public void runMRTFilesHandlerJob() {
        boolean filesFound = false;
        LocalDate today = LocalDate.ofInstant(Instant.now(), ZoneId.of("UTC"));
        if (!today.equals(this.today)) {
            this.today = today;
        }

        logger.debug("Starting the harmonieStatusFilesHandler");
        Span span = tracer.spanBuilder("runMRTFilesHandlerJob").startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.addEvent("Job started");
            File[] statusFiles = this.fileHandlingService.listFiles(this.inputPath);

            if (statusFiles != null && statusFiles.length > 0) {
                filesFound = true;
                span.addEvent("Processing input files", Attributes.of(
                        AttributeKey.stringKey("input_path"), this.inputPath,
                        AttributeKey.longKey("count"), (long)statusFiles.length
                ));

                logger.info("Found {} file(s) in the input folder", statusFiles.length);
                for (File statusFile : statusFiles) {
                    try {
                        if (statusFile.isDirectory()) {
                            logger.debug("Ignoring '{}' as it is a directory", statusFile.getAbsolutePath());
                            continue;
                        }

                        if (!statusFile.getAbsolutePath().toUpperCase().endsWith(".XML")) {
                            logger.warn("Unexpected file '{}' in the input directory. Moving to 'bin'", statusFile.getAbsolutePath());
                            this.fileHandlingService.moveFile(statusFile, this.binPath);
                            continue;
                        }

                        span.addEvent("Processing file", Attributes.of(
                                AttributeKey.stringKey("path"), statusFile.getAbsolutePath())
                        );
                        logger.info("Processing input metadata file '" + statusFile.getAbsolutePath() + "'");
                        span.addEvent("Reading status document");
                        StatusDocument document = this.fileHandlingService.readStatusDocument(statusFile.getAbsolutePath(), this.errorPath);

                        span.addEvent("Processing status document");

                        Timer t = this.timers.get(document.getStatus());
                        if (t == null) {
                            t = Timer.builder("harmonie_status_file_processing_time")
                                    .description("Harmonie status file processing time")
                                    .tag("status", document.getStatus())
                                    .register(this.registry);
                            this.timers.put(document.getStatus(), t);
                        }

                        t.recordCallable(() -> {
                            // Logic goes here
                            List<ResponseWrapper> rws = this.processStatusFile(statusFile.getName(), document, this.documentPath);

                            if (!rws.isEmpty()) {
                                span.addEvent("Status document has been processed");
                                logger.info("Metadata file '" + statusFile.getAbsolutePath() + "' has been processed. Moving it and the corresponding document file to the processing directory.");

                                span.addEvent("Moving status file to the processed directory", Attributes.of(
                                        AttributeKey.stringKey("processed_path"), this.processedPath));
                                String statusFilePath = this.fileHandlingService.moveFile(statusFile, this.processedPath);

                                span.addEvent("Updating status file paths");
                                // Update status file location for each status record
                                for (ResponseWrapper rw : rws) {
                                    try {
                                        span.addEvent("Updating status file path", Attributes.of(
                                                AttributeKey.stringKey("status_id"), rw.getStatusId(),
                                                AttributeKey.stringKey("status_file_path"), statusFilePath
                                        ));

                                        if (rw.getDocumentFile() != null) {
                                            this.fileHandlingService.moveFile(rw.getDocumentFile(), this.processedPath);
                                        }

                                        this.clientService.updateRequestStatusFilePath(rw.getStatusId(), statusFilePath);
                                    } catch (Throwable e) {
                                        span.recordException(e);
                                        span.setStatus(StatusCode.ERROR, "Cannot update request status '" + rw.getStatusId() + "' file '" + statusFile.getAbsolutePath() + "' path: " + e.getMessage());
                                        logger.error(String.format("Cannot update status file location to '%s' for the status Id '%s'", statusFilePath, rw.getStatusId()), e);
                                    }
                                }
                            }
                            return null;
                        });
                    } catch (ServiceException e) {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, "Cannot process status file '" + statusFile.getAbsolutePath() + "': " + e.getMessage());
                        int counter = 0;
                        logger.error("Cannot process file '" + statusFile.getAbsolutePath() + "': " + e.getMessage() + ".");

                        String errorPath = this.fileHandlingService.moveFile(statusFile, this.errorPath);
                        this.clientService.addStatusFileErrorRecord(statusFile.getName(), errorPath, e.getMessage());
                    }
                }
            }
            span.addEvent("Job completed");
            logger.debug("Harmonie Status File Handler Job execution has been completed");
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failure while processing incoming files: " + e.getMessage());
        } finally {
            if (filesFound) {
                logger.info("Completed Status Files Handler Job");
            }
            span.end();
        }
    }

    private List<ResponseWrapper> processStatusFile(String fileName, StatusDocument document, String printedDocumentsPath) throws ServiceException {
        List<ResponseWrapper> processedRequests = new ArrayList<>();
        Span span = tracer.spanBuilder("processStatusFile")
                .startSpan();

        Map<Long, Boolean> processedRequestIds= new HashMap<>();
        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("status", document.getStatus());
            if (document.getStatus().equals("PP")) {
                ProcessPrintedDocuments processPrintedDocuments = (d, p, attempts) -> {
                    if (attempts == 1) {
                        return false;
                    }

                    for (Request fileId : d.getPrintFile().getRequests()) {
                        File documentFile = new File(p, fileId.getRequestId() + ".pdf");
                        if (!documentFile.exists() || !documentFile.canRead()) {
                            return false;
                        }
                    }

                    try {
                        for (Request fileId : d.getPrintFile().getRequests()) {
                            if (processedRequestIds.containsKey(fileId.getRequestId())) {
                                continue;
                            }

                            File documentFile = null;
                            processedRequestIds.put(fileId.getRequestId(), true);
                            ClientRequestAndRoutingDetails details = this.clientService.getRequestIdAndRoutingDetails(fileId.getRequestId());
                            if (details == null) {
                                throw new ServiceException(String.format("Cannot find request Id for the file Id %d", fileId.getRequestId()));
                            }

                            ResponseBuilder rb = new ResponseBuilder(details.requestId());
                            rb
                                    .withHarmonieStatusCode(d.getStatus())
                                    .withStatusCreatedAt(d.getTimestamp());
                            documentFile = new File(p, fileId.getRequestId() + ".pdf");
                            if (!documentFile.exists()) {

                            }
                            Metadata metadata = new Metadata();
                            metadata.set_class("MedicalRisk");
                            metadata.set_type("DOCUMENT");
                            metadata.setMimeType("application/pdf");
                            metadata.set_name(details + ".pdf");
                            metadata.setCreationDate(LocalDateTime.now(ZoneId.of("UTC")));

                            AddDocumentResponse documentResponse = this.cmxClientService.addDocument(documentFile, metadata);

                            rb
                                    .withCMXDocumentId(documentResponse.getId())
                                    .withCMXDocumentCreatedAt(documentResponse.getMetadata().get_creationDate());

                            Response response = rb.build();

                            String statusId = this.clientService.addRequestStatus(response);
                            if (documentResponse != null) {
                                this.clientService.updateCMXDocumentId(details.requestId(), documentResponse.getId(), documentResponse.getMetadata().get_creationDate());
                            }

                            byte[] body = this.objectMapper.writeValueAsBytes(response);
                            logger.debug("Prepared the following message: {}", new String(body));

                            MessageBuilder mb = MessageBuilder.withBody(body);
                            mb.setMessageId(statusId);
                            mb.setContentType("application/json");
                            openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), mb, new TextMapSetter<MessageBuilder>() {
                                @Override
                                public void set(MessageBuilder carrier, String key, String value) {
                                    carrier.setHeader(key, value);
                                }
                            });
                            Message message = mb.build();

                            this.amqpTemplate.convertAndSend(details.responseExchange(), details.responseRoutingKey(), message);

                            processedRequests.add(new ResponseWrapper(response, statusId, documentFile));
                        }
                    } catch (ServiceException | AmqpException | JsonProcessingException e) {
                        logger.error("Cannot process Harmonie status XML file", e);
                        // Rolling back
                        for (ResponseWrapper wrapper : processedRequests) {
                            if (wrapper.getResponse().getDocumentId() != null) {
                                try {
                                    this.cmxClientService.deleteDocument(wrapper.getResponse().getDocumentId());
                                } catch (Throwable ex) {
                                    logger.error(String.format("Cannot delete CMX document with ID '%s'", wrapper.getResponse().getDocumentId()), ex);
                                }
                                try {
                                    this.clientService.updateCMXDocumentId(wrapper.getResponse().getRequestId(), null, null);
                                } catch (Throwable ex) {
                                    logger.error(String.format("Cannot reset CMX document ID in the request record, request Id: '%s'", wrapper.getResponse().getRequestId()), ex);
                                }
                            }
                            try {
                                this.clientService.deleteRequestStatusRecord(wrapper.getStatusId());
                            } catch (Throwable ex) {
                                logger.error(String.format("Cannot delete request status record, request Id '%s', status Id: '%s'", wrapper.getResponse().getRequestId(), wrapper.getStatusId()), ex);
                            }
                        }

                        if (e instanceof ServiceException) {
                            throw (ServiceException) e;
                        } else {
                            throw new ServiceException("Cannot process Harmonie status XML file: " + e.getMessage());
                        }
                    }

                    return true;
                };

                this.clientService.tryToProcessPrintedFile(fileName, document, printedDocumentsPath, processPrintedDocuments, this.maxAttempts);
            } else {
                for (Request fileId : document.getPrintFile().getRequests()) {
                    if (processedRequestIds.containsKey(fileId.getRequestId())) {
                        continue;
                    }

                    File documentFile = null;
                    processedRequestIds.put(fileId.getRequestId(), true);
                    ClientRequestAndRoutingDetails details = this.clientService.getRequestIdAndRoutingDetails(fileId.getRequestId());
                    if (details == null) {
                        throw new ServiceException(String.format("Cannot find request Id for the file Id %d", fileId.getRequestId()));
                    }

                    ResponseBuilder rb = new ResponseBuilder(details.requestId());
                    rb
                            .withHarmonieStatusCode(document.getStatus())
                            .withStatusCreatedAt(document.getTimestamp());

                    Response response = rb.build();

                    String statusId = this.clientService.addRequestStatus(response);
                    byte[] body = this.objectMapper.writeValueAsBytes(response);
                    logger.debug("Prepared the following message: {}", new String(body));

                    MessageBuilder mb = MessageBuilder.withBody(body);
                    mb.setMessageId(statusId);
                    mb.setContentType("application/json");
                    openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), mb, new TextMapSetter<MessageBuilder>() {
                        @Override
                        public void set(MessageBuilder carrier, String key, String value) {
                            carrier.setHeader(key, value);
                        }
                    });
                    Message message = mb.build();

                    this.amqpTemplate.convertAndSend(details.responseExchange(), details.responseRoutingKey(), message);

                    processedRequests.add(new ResponseWrapper(response, statusId, documentFile));
                }
            }
        } catch(ServiceException | AmqpException | JsonProcessingException e) {
            logger.error("Cannot process Harmonie status XML file", e);
            // Rolling back
            for (ResponseWrapper wrapper : processedRequests) {
                try {
                    this.clientService.deleteRequestStatusRecord(wrapper.getStatusId());
                } catch (Throwable ex) {
                    logger.error(String.format("Cannot delete request status record, request Id '%s', status Id: '%s'", wrapper.getResponse().getRequestId(), wrapper.getStatusId()), ex);
                }
            }

            if (e instanceof ServiceException) {
                throw (ServiceException) e;
            } else {
                throw new ServiceException(e.getMessage());
            }
        } finally {
            span.end();
        }

        return processedRequests;
    }
}
