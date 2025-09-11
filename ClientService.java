package axa.partners.clp.mrtdocumentbridge.service;

import axa.partners.clp.mrtdocumentbridge.data.ClientRequestAndRoutingDetails;
import axa.partners.clp.mrtdocumentbridge.data.Document;
import axa.partners.clp.mrtdocumentbridge.data.Request;
import axa.partners.clp.mrtdocumentbridge.data.Response;
import axa.partners.clp.mrtdocumentbridge.data.harmonie.StatusDocument;
import axa.partners.clp.mrtdocumentbridge.data.internal.ClientConfiguration;
import axa.partners.clp.mrtdocumentbridge.exception.ServiceException;
import axa.partners.clp.mrtdocumentbridge.util.DBUtil;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is a collection methods that is storing or retrieving data from the database
 */
@Service
public class ClientService {
    private static final Logger logger = LoggerFactory.getLogger(ClientService.class);

    private final DataSource dataSource;

    private final Tracer tracer;

    private final AtomicInteger failedPrintedFiles;

    /**
     * Constructs a new ClientService object.
     *
     * @param dataSource the data source used for database operations
     */
    @Autowired
    public ClientService(DataSource dataSource,
                         MeterRegistry registry,
                         OpenTelemetry openTelemetry,
                         String version) {
        this.dataSource = dataSource;
        this.failedPrintedFiles = new AtomicInteger(0);
        Gauge.builder("printed_document.failures", () -> this.failedPrintedFiles)
                .description("Number of failed printed documents")
                .register(registry);
        this.tracer = openTelemetry.getTracer(ClientService.class.getName(), version);
    }

    /**
     * Retrieves the list of enabled clients from the database.
     *
     * @return a List of ClientConfiguration objects representing the enabled clients
     * @throws ServiceException if an error occurs while retrieving the clients
     */
    public List<ClientConfiguration> getEnabledClients() throws ServiceException {
        List<ClientConfiguration> result = new ArrayList<>();

        Span span = this.tracer.spanBuilder("getEnabledClients")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            try (Connection conn = this.dataSource.getConnection()) {
                try (Statement st = conn.createStatement()) {
                    try (ResultSet rs = st.executeQuery(
                            """
                                    SELECT client_id, soap_endpoint, response_exchange, response_routing_key
                                    FROM CLIENT_CONFIGURATION WHERE is_enabled = true ORDER BY client_id
                                """)) {
                        while (rs.next()) {
                            ClientConfiguration clientConfiguration = new ClientConfiguration(
                                    rs.getString(1),
                                    true,
                                    rs.getString(2),
                                    rs.getString(3),
                                    rs.getString(4));
                            result.add(clientConfiguration);
                        }
                        span.setStatus(StatusCode.OK);
                    }
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot retrieve list of active clients: {}", e.getMessage());
                throw new ServiceException(String.format("Cannot retrieve list of active clients: %s", e.getMessage()));
            }
        } finally {
            span.end();
        }

        return result;
    }

    /**
     * Retrieves the request ID associated with the given file ID.
     *
     * @param fileId the file ID for which the request ID is to be retrieved
     * @return the request ID associated with the given file ID, or null if no request ID is found
     * @throws ServiceException if an error occurs while retrieving the request ID
     */
    public ClientRequestAndRoutingDetails getRequestIdAndRoutingDetails(long fileId) throws ServiceException {
        Span span = this.tracer
                .spanBuilder("getRequestId")
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("file_id", fileId);
            try (Connection conn = this.dataSource.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("""
                    SELECT r.id, r.client_id, c.response_exchange, c.response_routing_key
                    FROM CLIENT_REQUEST r inner join CLIENT_CONFIGURATION c
                    ON r.client_id = c.client_id
                    WHERE file_id = ?
                """)) {
                    st.setLong(1, fileId);
                    try (ResultSet rs = st.executeQuery()) {
                        if (rs.next()) {
                            ClientRequestAndRoutingDetails details = new ClientRequestAndRoutingDetails(
                                    rs.getString(1),
                                    rs.getString(2),
                                    rs.getString(3),
                                    rs.getString(4));
                            span.setAttribute("request_id", details.requestId());
                            span.setAttribute("response_exchange", details.responseExchange());
                            span.setAttribute("response_routing_key", details.responseRoutingKey());
                            return details;
                        }
                        span.setStatus(StatusCode.OK);
                    }
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot retrieve request Id for the file Id {}: {}", fileId, e.getMessage());
                throw new ServiceException(String.format("Cannot retrieve request Id for the file Id %d: %s", fileId, e.getMessage()));
            }
        } finally {
            span.end();
        }

        return null;
    }

    /**
     * Adds a request status record to the request_status_history table.
     *
     * @param response The response object containing the request ID, status code, and message
     * @return The ID of the newly created request status record
     * @throws ServiceException if an error occurs while adding the request status record
     */
    public String addRequestStatus(Response response) throws ServiceException {
        String requestId = UUID.randomUUID().toString();
        LocalDateTime lastStatusUpdateDateTime = null;

        Span span = this.tracer
                .spanBuilder("addRequestStatus")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("request_id", response.getRequestId());
            span.setAttribute("status", response.getStatusCode());

            try (Connection conn = this.dataSource.getConnection()) {
                DBUtil.setAutoCommit(conn, false);

                try (PreparedStatement getLatestStatusHistoryItemStatement = conn.prepareStatement("""
                        SELECT status_generated_at from request_status_history WHERE request_id = ?::uuid ORDER BY status_generated_at desc LIMIT 1
                    """)) {
                    getLatestStatusHistoryItemStatement.setString(1, response.getRequestId());
                    try (ResultSet getLatestStatusHistoryItemResultSet = getLatestStatusHistoryItemStatement.executeQuery()) {
                        if (getLatestStatusHistoryItemResultSet.next()) {
                            lastStatusUpdateDateTime = getLatestStatusHistoryItemResultSet.getTimestamp(1).toLocalDateTime();
                        }
                    }
                }

                try (PreparedStatement insertRequestStatusHistoryStatement = conn.prepareStatement("""
                            INSERT INTO request_status_history (id, status_generated_at, created_at, request_id, status, message)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """)) {
                    insertRequestStatusHistoryStatement.setObject(1, requestId, Types.OTHER);
                    insertRequestStatusHistoryStatement.setTimestamp(2, Timestamp.valueOf(response.getStatusCreatedAt()));
                    insertRequestStatusHistoryStatement.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
                    insertRequestStatusHistoryStatement.setObject(4, response.getRequestId(), Types.OTHER);
                    insertRequestStatusHistoryStatement.setString(5, response.getStatusCode());
                    insertRequestStatusHistoryStatement.setString(6, response.getMessage());
                    insertRequestStatusHistoryStatement.executeUpdate();

                    if (lastStatusUpdateDateTime == null || lastStatusUpdateDateTime.isBefore(response.getStatusCreatedAt())) {
                        try (PreparedStatement updateRequestStatusStatement = conn.prepareStatement("""
                                    UPDATE CLIENT_REQUEST set status = ? WHERE ID = ?::uuid
                                """)) {
                            updateRequestStatusStatement.setString(1, response.getStatusCode());
                            updateRequestStatusStatement.setString(2, response.getRequestId());
                            updateRequestStatusStatement.executeUpdate();
                            logger.debug("Updated request '" + requestId + "' with status '" + response.getStatusCode() + "'");
                        }
                    }
                }
                DBUtil.commit(conn);
                span.setStatus(StatusCode.OK);
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot create request status record for the request Id '{}': {}", response.getRequestId(), e.getMessage());
                throw new ServiceException(String.format("Cannot create request status record for the request Id '%s': %s", response.getRequestId(), e.getMessage()));
            }
        } finally {
            span.end();
        }

        return requestId;
    }

    /**
     * Checks if a client request exists. Client request comprises of the batch Id optional and set of requests Ids.
     * The rule for validation here is that neither of the Ids must exist in the database. If any of them found the request
     * is rejected.
     *
     * @param request the document creation request
     * @return true if the client request exists, false otherwise
     * @throws ServiceException if an error occurs while checking the client request's existence
     */
    public boolean checkIfClientRequestExists(Request request) throws ServiceException {
        PreparedStatement stValidateRequest = null;
        ResultSet rsValidateRequest = null;

        Span span = this.tracer
                .spanBuilder("checkIfClientRequestExists")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            // Make sure that documents list is not empty
            if (request.getDocuments() == null || request.getDocuments().isEmpty()) {
                throw new ServiceException("Empty request");
            }

            if (request.getBatchId() == null && request.getDocuments().size() > 1) {
                throw new ServiceException("Expect request with a single document in the 'Interactive' mode");
            }

            try (Connection conn = this.dataSource.getConnection()) {
                if (request.getBatchId() != null) {
                    span.setAttribute("batch_id", request.getBatchId());
                    try (PreparedStatement st = conn.prepareStatement("""
                        SELECT ID FROM CLIENT_REQUEST_BATCH WHERE ID = ?::uuid
                    """)) {
                        st.setString(1, request.getBatchId());
                        try (ResultSet rs = st.executeQuery()) {
                            if (rs.next()) {
                                logger.debug(String.format("Batch '%s' already exists.", request.getBatchId()));
                                span.setStatus(StatusCode.OK);
                                return true;
                            }
                        }
                    }
                }

                int index = 0;
                for (Document document : request.getDocuments()) {
                    span.setAttribute("request_id_" + index++, document.getRequestId());
                    try (PreparedStatement st = conn.prepareStatement("""
                                SELECT id FROM client_request WHERE id = ?::uuid
                            """)) {
                        st.setString(1, document.getRequestId());
                        try (ResultSet rs = st.executeQuery()) {
                            if (rs.next()) {
                                logger.debug(String.format("Request '%s' already exists.", document.getRequestId()));
                                span.setStatus(StatusCode.OK);
                                return true;
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                if (request.getBatchId() != null) {
                    logger.error("Cannot validate if requests for the batch '{}' does already exist: {}", request.getBatchId(), e.getMessage());
                    throw new ServiceException(String.format("Cannot validate if requests for the batch '%s' does already exist: %s", request.getBatchId(), e.getMessage()));
                } else {
                    logger.error("Cannot validate if request '{}' does already exist: {}", request.getDocuments().getFirst().getRequestId(), e.getMessage());
                    throw new ServiceException(String.format("Cannot validate if request '%s' does already exist: %s", request.getDocuments().getFirst().getRequestId(), e.getMessage()));
                }
            }
            span.setStatus(StatusCode.OK);
        } finally {
            span.end();
        }

        return false;
    }

    /**
     * This method adds a new record to a `client_request` table
     * @param request client request details
     * @param status initial status
     * @throws ServiceException if any error occurs during processing
     */
    public void addClientRequest(Request request, String status) throws ServiceException {

        Span span = this.tracer
                .spanBuilder("addClientRequest")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            try (Connection conn = this.dataSource.getConnection()) {
                conn.setAutoCommit(false);
                if (request.getBatchId() != null) {
                    span.setAttribute("batch_id", request.getBatchId());
                    try (PreparedStatement st = conn.prepareStatement("INSERT INTO CLIENT_REQUEST_BATCH (ID, CREATED_AT) VALUES (?::uuid, ?)")) {
                        st.setString(1, request.getBatchId());
                        st.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                        st.executeUpdate();
                        logger.debug(String.format("Created batch '%s'", request.getBatchId()));
                    }
                }

                int index = 0;
                for (Document document : request.getDocuments()) {
                    span.setAttribute("request_id_" + index, document.getRequestId());
                    span.setAttribute("file_id_" + index++, document.getFileId());

                    try (PreparedStatement st = conn.prepareStatement("""
                            INSERT INTO client_request (id, batch_id, created_at, file_id, document_type, command, client_id, status)
                            VALUES (?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?)
                        """)) {
                        st.setObject(1, document.getRequestId(), Types.OTHER);
                        if (request.getBatchId() != null) {
                            st.setString(2, request.getBatchId());
                        } else {
                            st.setNull(2, Types.OTHER);
                        }
                        st.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
                        st.setLong(4, document.getFileId());
                        st.setString(5, document.getDocumentType());
                        st.setString(6, request.getCommand());
                        st.setString(7, request.getClientId());
                        st.setString(8, status);
                        st.executeUpdate();
                        logger.debug(String.format("Created request '%s'", document.getRequestId()));
                    }
                }
                conn.commit();
                logger.debug("Committed addClientRequest transaction");
                span.setStatus(StatusCode.OK);
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot create request record: {}", e.getMessage());
                throw new ServiceException(String.format("Cannot create request record: %s", e.getMessage()));
            }
        } finally {
            span.end();
        }
    }

    /**
     * This method adds a new record to the `status_file_error` table.
     *
     * @param fileName the name of the file with error
     * @param filePath the path of the file with error
     * @param message the error message
     * @throws ServiceException if any error occurs during processing
     */
    public void addStatusFileErrorRecord(String fileName, String filePath, String message) throws ServiceException {
        Span span = this.tracer.spanBuilder("addStatusFileErrorRecord")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("file_name", fileName);
            span.setAttribute("file_path", filePath);
            span.setAttribute("message", message);
            try (Connection conn = this.dataSource.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("""
                    INSERT INTO status_file_error (id, created_at, file_name, file_path, message)
                    VALUES (?, ?, ?, ?, ?)
                """)) {
                    st.setObject(1, UUID.randomUUID().toString(), Types.OTHER);
                    st.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                    st.setString(3, fileName);
                    st.setString(4, filePath);
                    st.setString(5, message);
                    st.executeUpdate();
                    span.setStatus(StatusCode.OK);
                }
            } catch (SQLException e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR);
                logger.error("Cannot create status file error record for the file '{}': {}", fileName, e.getMessage());
                throw new ServiceException(String.format("Cannot create status file error record for the file '%s': %s", fileName, e.getMessage()));
            }
        } finally {
            span.end();
        }

        return;
    }

    /**
     * This method updates the file path for a specific request status entry in the `request_status_history` table.
     *
     * @param id the id of the request status entry
     * @param path the new file path to be updated
     * @throws ServiceException if any error occurs during processing
     */
    public void updateRequestStatusFilePath(String id, String path) throws ServiceException {
        Span span = this.tracer.spanBuilder("updateRequestStatusFilePath")
                .startSpan();

        span.setAttribute("status_id", id);
        span.setAttribute("path", path);
        try(Scope scope = span.makeCurrent()) {
            try (Connection conn = this.dataSource.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("""
                    UPDATE request_status_history set file_path=? WHERE id = ?::uuid
                """)) {
                    st.setString(1, path);
                    st.setString(2, id);
                    st.executeUpdate();
                    span.setStatus(StatusCode.OK);
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot store the status file path for the status Id - '{}': {}", id, e.getMessage());
                throw new ServiceException(String.format("Cannot store the status file path for the status Id - '%s': %s", id, e.getMessage()));
            }
        } finally {
            span.end();
        }
    }

    /**
     * This method updates the CMX Document ID and CMX Document Created At timestamp for a specific client request in the `client_request` table.
     *
     * @param id the id of the client request
     * @param cmxDocumentId the new CMX Document ID to be updated
     * @param documentCreatedAt the new CMX Document Created At timestamp to be updated
     * @throws ServiceException if any error occurs during processing
     */
    public void updateCMXDocumentId(String id, String cmxDocumentId, LocalDateTime documentCreatedAt) throws ServiceException {
        Span span = this.tracer
                .spanBuilder("updateCMXDocumentId")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("request_id", id);
            span.setAttribute("cmx_document_id", cmxDocumentId);

            try (Connection conn = this.dataSource.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("""
                    UPDATE client_request set cmx_document_id=?, cmx_document_created_at = ? WHERE id = ?::uuid
                """)) {
                    st.setString(1, cmxDocumentId);
                    if (documentCreatedAt != null) {
                        st.setTimestamp(2, Timestamp.valueOf(documentCreatedAt));
                    } else {
                        st.setNull(2, Types.TIMESTAMP);
                    }
                    st.setString(3, id);
                    st.executeUpdate();
                    span.setStatus(StatusCode.OK);
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot store the CMX Document ID for the request Id - '{}': {}", id, e.getMessage());
                throw new ServiceException(String.format("Cannot store the CMX Document ID for the request Id - '%s': %s", id, e.getMessage()));
            }
        } finally {
            span.end();
        }
    }

    /**
     * This method deletes a specific request status record from the `request_status_history` table.
     *
     * @param id the id of the request status record to delete
     * @throws ServiceException if any error occurs during processing
     */
    public void deleteRequestStatusRecord(String id) throws ServiceException {
        Span span = this.tracer
                .spanBuilder("deleteRequestStatusRecord")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("request_id", id);
            try (Connection conn = this.dataSource.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("""
                    DELETE FROM request_status_history WHERE id = ?::uuid
                """)) {
                    st.setString(1, id);
                    st.executeUpdate();
                    span.setStatus(StatusCode.OK);
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot delete the status record with Id - '{}': {}", id, e.getMessage());
                throw new ServiceException(String.format("Cannot delete the status record with Id - '%s': %s", id, e.getMessage()));
            }
        } finally {
            span.end();
        }
    }

    /**
     * This method adds a message to the parking store for further processing.
     *
     * @param message the message to add to the parking store
     * @throws ServiceException if any error occurs during processing
     */
    public void addMessageToParkingStore(Message message, String originalExchange, String originalRoutingKey) throws ServiceException {
        Span span = this.tracer
                .spanBuilder("addMessageToParkingStore")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("message_id", message.getMessageProperties().getMessageId());
            span.setAttribute("original_exchange", originalExchange);
            span.setAttribute("original_routing_key", originalRoutingKey);

            try (Connection conn = this.dataSource.getConnection()) {
                DBUtil.setAutoCommit(conn, false);
                try (PreparedStatement stAddParkingLotMessage = conn.prepareStatement("""
                    INSERT INTO amqp_parking (message_id, created_at, original_exchange, original_routing_key, payload)
                    VALUES (?, ?, ?, ?, ?)
                """)) {
                    try (PreparedStatement stAddParkingLotMessageHeader = conn.prepareStatement("""
                                INSERT INTO amqp_parking_header (message_id, header, value)
                                VALUES (?, ?, ?)
                            """)) {
                        String messageId = message.getMessageProperties().getMessageId();
                        if (messageId == null) {
                            messageId = UUID.randomUUID().toString();
                        }

                        stAddParkingLotMessage.setString(1, messageId);
                        stAddParkingLotMessage.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                        stAddParkingLotMessage.setString(3, originalExchange);
                        stAddParkingLotMessage.setString(4, originalRoutingKey);
                        stAddParkingLotMessage.setBytes(5, message.getBody());
                        stAddParkingLotMessage.executeUpdate();

                        for (String key : message.getMessageProperties().getHeaders().keySet()) {
                            stAddParkingLotMessageHeader.clearParameters();
                            stAddParkingLotMessageHeader.setString(1, messageId);
                            stAddParkingLotMessageHeader.setString(2, key);
                            stAddParkingLotMessageHeader.setString(3, message.getMessageProperties().getHeader(key).toString());
                            stAddParkingLotMessageHeader.executeUpdate();
                        }
                        DBUtil.commit(conn);
                        span.setStatus(StatusCode.OK);
                    }
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot store AMQP message retrieved from the Parking Lot Queue. Message ID: '{}': {}", message.getMessageProperties().getMessageId(), e.getMessage());
                throw new ServiceException(String.format("Cannot store AMQP message retrieved from the Parking Lot Queue. Message ID: '%s': %s", message.getMessageProperties().getMessageId(), e.getMessage()));
            }
        } finally {
            span.end();
        }
    }

    /**
     * This method retrieves the client configuration for a given client name.
     *
     * @param clientName the name of the client to retrieve the configuration for
     * @return the client configuration
     */
    public ClientConfiguration getClientConfiguration(String clientName) throws ServiceException {
        Span span = this.tracer
                .spanBuilder("getClientConfiguration")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("client_id", clientName);

            try (Connection conn = this.dataSource.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("""
                    SELECT client_id, soap_endpoint, response_exchange, response_routing_key
                    FROM CLIENT_CONFIGURATION WHERE client_id = ?
                """)) {
                    st.setString(1, clientName);
                    try (ResultSet rs = st.executeQuery()) {
                        if (rs.next()) {
                            return new ClientConfiguration(
                                    rs.getString(1),
                                    true,
                                    rs.getString(2),
                                    rs.getString(3),
                                    rs.getString(4));
                        }
                        span.setStatus(StatusCode.OK);
                    }
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Cannot retrieve client '{}' configuration: {}", clientName, e.getMessage());
                throw new ServiceException(String.format("Cannot retrieve client '%s' configuration: %s", clientName, e.getMessage()));
            }
        } finally {
            span.end();
        }

        return null;
    }

    /**
     * This method tries to process printed "PP" status file. It will first lock the file record.
     * Then attempt to process files and if successful just delete record, otherwise increment attempts value.
     *
     * @param statusFileName the name of the status file to process
     */
    public void tryToProcessPrintedFile(String statusFileName, StatusDocument document, String printedDocumentsPath, ProcessPrintedDocuments process, int maxAttempts) throws ServiceException {
        logger.info("Processing status file");
        Span span = this.tracer
                .spanBuilder("getClientConfiguration")
                .startSpan();

        try(Scope scope = span.makeCurrent()) {
            span.setAttribute("client_id", statusFileName);

            try (Connection conn = this.dataSource.getConnection()){
                try (PreparedStatement st = conn.prepareStatement("""
                            SELECT status_file_name, attempts
                            FROM document_processing_log WHERE status_file_name = ?
                            FOR UPDATE
                        """,
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                    st.setString(1, statusFileName);
                    try (ResultSet rs = st.executeQuery()) {
                        if (rs.next()) {
                            int attempts = rs.getInt(2);
                            try {
                                if (!process.processPrintedStatusFile(document, printedDocumentsPath, ++attempts)) {
                                    if (attempts < maxAttempts) {
                                        rs.updateInt(2, attempts);
                                        rs.updateRow();
                                    } else {
                                        rs.deleteRow();
                                        this.failedPrintedFiles.addAndGet(1);
                                        logger.error("Could not process '{}' status file after {} attempts. Giving up...", statusFileName, attempts);
                                        throw new ServiceException(String.format("Could not process '%s' status file after %d attempts. Giving up...", statusFileName, attempts));
                                    }
                                } else {
                                    rs.deleteRow();
                                }
                            } catch (ServiceException e) {
                                rs.deleteRow();
                                throw e;
                            }
                        } else {
                            try (PreparedStatement createFileRecord = conn.prepareStatement("""
                                INSERT INTO document_processing_log (status_file_name) VALUES (?)
                            """))  {
                                createFileRecord.setString(1, statusFileName);
                                createFileRecord.executeUpdate();
                            }
                            process.processPrintedStatusFile(document, printedDocumentsPath, 1);
                        }
                    }
                }
            } catch (SQLException e) {
                span.setStatus(StatusCode.ERROR);
                span.recordException(e);
                logger.error("Database exception: {}", e.getMessage());
                throw new ServiceException(String.format("Cannot retrieve client '%s' configuration: %s", statusFileName, e.getMessage()));
            }
        }
    }

    public boolean validateDbConnection() {
        try (Connection conn = this.dataSource.getConnection()) {
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT 1")) {
                    if (rs.next()) {
                        if (rs.getInt(1) == 1) {
                            return true;
                        }
                    }
                }
            }
        } catch (Throwable e) {
            logger.error("Connection is not available: {}", e.getMessage());
            return false;
        }
        return false;
    }
}
