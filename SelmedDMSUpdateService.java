package axa.partners.clp.dms.migration.service;

import axa.partners.clp.dms.migration.data.ApplicationConfiguration;
import axa.partners.clp.dms.migration.data.LoanIds;
import axa.partners.clp.dms.migration.data.decisions.*;
import axa.partners.clp.dms.migration.data.dms.*;
import axa.partners.clp.dms.migration.data.jdbc.Field;
import axa.partners.clp.dms.migration.exception.DecisionProcessingException;
import axa.partners.clp.dms.migration.exception.ServiceException;
import axa.partners.clp.dms.migration.util.DBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.*;

import static axa.partners.clp.dms.migration.util.DBUtil.*;

@Service
public class SelmedDMSUpdateService {
    private static final Logger logger = LoggerFactory.getLogger(SelmedDMSUpdateService.class);
    private final DataSource dataSource;
    private final DataSource risksDataSource;
    private final DataSource decisionsDataSource;
    private final ApplicationConfiguration applicationConfiguration;

    @Value("${dms.application.size:10000}")
    private int applicationSize;

    @Value("${dms.coverages.size:10000}")
    private int coveragesSize;

    @Autowired
    public SelmedDMSUpdateService(DataSource dataSource,
                                  DataSource risksDataSource,
                                  ApplicationConfiguration applicationConfiguration,
                                  DataSource decisionsDataSource) {
        this.dataSource = dataSource;
        this.risksDataSource = risksDataSource;
        this.applicationConfiguration = applicationConfiguration;
        this.decisionsDataSource = decisionsDataSource;
    }

    public void createDMSRisksApplicationFileUpdatePackage(Connection conn, String operation, SelmedDMSRisksApplicationFileUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_RAF_UPDATE_PACKAGE (ID, CREATED_AT, OPERATION, FILE_NUMBER, CREATION_DATE, MODIFICATION_DATE, IS_LOCKED, IS_ARCHIVED, STATUS, RAF_ID) VALUES (nextval('selmed_dms_raf_update_package_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID");
            ps.setTimestamp(1, createdAt);
            ps.setString(2, operation);
            ps.setString(3, update.getFileNumber());
            if (update.getCreationDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(update.getCreationDate()));
            } else {
                ps.setNull(4, Types.TIMESTAMP);
            }

            if (update.getModificationDate() != null) {
                ps.setTimestamp(5, Timestamp.valueOf(update.getModificationDate()));
            } else {
                ps.setNull(5, Types.TIMESTAMP);
            }

            if (update.isLocked() != null) {
                ps.setBoolean(6, update.isLocked());
            } else {
                ps.setNull(6, Types.BOOLEAN);
            }

            if (update.isArchived() != null) {
                ps.setBoolean(7, update.isArchived());
            } else {
                ps.setNull(7, Types.BOOLEAN);
            }

            ps.setString(8, "ADDED");
            ps.setString(9, update.getApplicationId());
            rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                logger.debug("createDMSRisksApplicationFileUpdatePackage - insert into SELMED_DMS_RAF_UPDATE_PACKAGE with fileNumber - {} is successful. New Id - {}",
                        update.getFileNumber(), id);
                this.addDMSRisksApplicationFileUpdatePackageHistoryItem(conn, id, createdAt, "ADDED", null);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("createDMSRisksApplicationFileUpdatePackage: Error inserting in SELMED_DMS_RAF_UPDATE_PACKAGE with fileNumber - %s, message - %s",
                    update.getFileNumber(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }
    }

    private void addDMSRisksApplicationFileUpdatePackageHistoryItem(Connection conn, long id, Timestamp modifiedAt, String status, String message) throws ServiceException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_RAF_UPDATE_PACKAGE_HISTORY (ID, MODIFIED_AT, STATUS, MESSAGE) VALUES (?, ?, ?, ?)");
            ps.setLong(1, id);
            ps.setTimestamp(2, modifiedAt);
            ps.setString(3, status);
            ps.setString(4, message);
            ps.executeUpdate();
            logger.debug("addDMSRisksApplicationFileUpdatePackageHistoryItem - insert into SELMED_DMS_RAF_UPDATE_PACKAGE_HISTORY with id - {} is successful", id);
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "addDMSRisksApplicationFileUpdatePackageHistoryItem: Error inserting in SELMED_DMS_RAF_UPDATE_PACKAGE_HISTORY with id - %s, message - %s",
                    id, e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
    }

    public void createDMSDecisionsApplicationFileUpdatePackage(Connection conn, String operation, SelmedDMSDecisionsApplicationFileUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_DAF_UPDATE_PACKAGE (ID, CREATED_AT, OPERATION, FILE_NUMBER, CREATION_DATE, MODIFICATION_DATE, IS_LOCKED, IS_ARCHIVED, HAS_MEDICAL_RISK_SHEET, STATUS, DAF_ID) VALUES (nextval('selmed_dms_raf_update_package_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID");
            ps.setTimestamp(1, createdAt);
            ps.setString(2, operation);
            ps.setString(3, update.getFileNumber());
            if (update.getCreationDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(update.getCreationDate()));
            } else {
                ps.setNull(4, Types.TIMESTAMP);
            }

            if (update.getModificationDate() != null) {
                ps.setTimestamp(5, Timestamp.valueOf(update.getModificationDate()));
            } else {
                ps.setNull(5, Types.TIMESTAMP);
            }

            if (update.isLocked() != null) {
                ps.setBoolean(6, update.isLocked());
            } else {
                ps.setNull(6, Types.BOOLEAN);
            }

            if (update.isLocked() != null) {
                ps.setBoolean(7, update.isLocked());
            } else {
                ps.setNull(7, Types.BOOLEAN);
            }

            if (update.hasMedicalRiskSheet() != null) {
                ps.setBoolean(8, update.hasMedicalRiskSheet());
            } else {
                ps.setNull(8, Types.BOOLEAN);
            }

            ps.setString(9, "ADDED");
            ps.setString(10, update.getApplicationId());
            rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                logger.debug("createDMSDecisionsApplicationFileUpdatePackage - insert into SELMED_DMS_DAF_UPDATE_PACKAGE with fileNumber - {} is successful. New Id - {}",
                        update.getFileNumber(), id);
                this.addDMSDecisionsApplicationFileUpdatePackageHistoryItem(conn, id, createdAt, "ADDED", null);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("createDMSDecisionsApplicationFileUpdatePackage: Error inserting in SELMED_DMS_DAF_UPDATE_PACKAGE with fileNumber - %s, message - %s",
                    update.getFileNumber(), e.getMessage()));

        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }
    }

    private void addDMSDecisionsApplicationFileUpdatePackageHistoryItem(Connection conn, long id, Timestamp modifiedAt, String status, String message) throws ServiceException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_DAF_UPDATE_PACKAGE_HISTORY (ID, MODIFIED_AT, STATUS, MESSAGE) VALUES (?, ?, ?, ?)");
            ps.setLong(1, id);
            ps.setTimestamp(2, modifiedAt);
            ps.setString(3, status);
            ps.setString(4, message);
            ps.executeUpdate();
            logger.debug("addDMSDecisionsApplicationFileUpdatePackageHistoryItem - insert into SELMED_DMS_DAF_UPDATE_PACKAGE_HISTORY with id - {} is successful", id);
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "addDMSDecisionsApplicationFileUpdatePackageHistoryItem: Error inserting in SELMED_DMS_DAF_UPDATE_PACKAGE_HISTORY with id - %s, message - %s",
                    id, e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
    }

    public void createDMSRisksUpdatePackage(Connection conn, String operation, SelmedDMSRisksUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_RISKS_UPDATE_PACKAGE (ID, CREATED_AT, OPERATION, RISK_ID, CREATION_DATE, MODIFICATION_DATE, RISK_TYPE, FILE_NUMBER, STATUS) VALUES (nextval('selmed_dms_risks_update_package_seq'), ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID");
            ps.setTimestamp(1, createdAt);
            ps.setString(2, operation);
            ps.setString(3, update.getRiskId());
            if (update.getCreationDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(update.getCreationDate()));
            } else {
                ps.setNull(4, Types.TIMESTAMP);
            }

            if (update.getModificationDate() != null) {
                ps.setTimestamp(5, Timestamp.valueOf(update.getModificationDate()));
            } else {
                ps.setNull(5, Types.TIMESTAMP);
            }

            if (update.getRiskType() != null) {
                ps.setInt(6, update.getRiskType());
            } else {
                ps.setNull(6, Types.INTEGER);
            }

            if (update.getFileNumber() != null) {
                ps.setString(7, update.getFileNumber());
            } else {
                ps.setNull(7, Types.VARCHAR);
            }

            ps.setString(8, "ADDED");
            rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                logger.debug("createDMSRisksUpdatePackage - insert into SELMED_DMS_RISKS_UPDATE_PACKAGE with riskId - {} is successful. New Id - {}",
                        update.getRiskId(), id);
                this.addDMSRisksUpdatePackageHistoryItem(conn, id, createdAt, "ADDED", null);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("createDMSRisksUpdatePackage: Error inserting in SELMED_DMS_RISKS_UPDATE_PACKAGE with riskId - %s, message - %s",
                    update.getRiskId(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }
    }

    private void addDMSRisksUpdatePackageHistoryItem(Connection conn, long id, Timestamp modifiedAt, String status, String message) throws ServiceException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_RISKS_UPDATE_PACKAGE_HISTORY (ID, MODIFIED_AT, STATUS, MESSAGE) VALUES (?, ?, ?, ?)");
            ps.setLong(1, id);
            ps.setTimestamp(2, modifiedAt);
            ps.setString(3, status);
            ps.setString(4, message);
            ps.executeUpdate();
            logger.debug("addDMSRisksUpdatePackageHistoryItem - insert into SELMED_DMS_RISKS_UPDATE_PACKAGE_HISTORY with id - {} is successful", id);
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("addDMSRisksUpdatePackageHistoryItem: Error inserting in SELMED_DMS_RISKS_UPDATE_PACKAGE_HISTORY with id - %s, message - %s ",
                    id, e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
    }

    public void createDMSNotesUpdatePackage(Connection conn, String operation, SelmedDMSNotesUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_NOTES_UPDATE_PACKAGE (ID, CREATED_AT, OPERATION, NOTE_ID, CREATION_DATE, MODIFICATION_DATE, DESCRIPTION, AUTHOR_ID, NOTE_TYPE, RISK_ID, STATUS, FILE_NUMBER) VALUES (nextval('selmed_dms_risks_update_package_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID");
            ps.setTimestamp(1, createdAt);
            ps.setString(2, operation);
            ps.setString(3, update.getNoteId());
            if (update.getCreationDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(update.getCreationDate()));
            } else {
                ps.setNull(4, Types.TIMESTAMP);
            }

            if (update.getModificationDate() != null) {
                ps.setTimestamp(5, Timestamp.valueOf(update.getModificationDate()));
            } else {
                ps.setNull(5, Types.TIMESTAMP);
            }

            if (update.getDescription() != null) {
                ps.setString(6, update.getDescription());
            } else {
                ps.setNull(6, Types.VARCHAR);
            }

            if (update.getAuthorId() != null) {
                ps.setString(7, update.getAuthorId());
            } else {
                ps.setNull(7, Types.VARCHAR);
            }

            if (update.getNoteType() != null) {
                ps.setInt(8, update.getNoteType());
            } else {
                ps.setNull(8, Types.INTEGER);
            }

            if (update.getRiskId() != null) {
                ps.setString(9, update.getRiskId());
            } else {
                ps.setNull(9, Types.VARCHAR);
            }

            ps.setString(10, "ADDED");
            ps.setString(11, update.getFileNumber());
            rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                logger.debug("createDMSNotesUpdatePackage - insert into SELMED_DMS_NOTES_UPDATE_PACKAGE with noteId - {} is successful. New Id - {}",
                        update.getNoteId(), id);
                this.addDMSNotesUpdatePackageHistoryItem(conn, id, createdAt, "ADDED", null);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("createDMSNotesUpdatePackage: Error inserting in SELMED_DMS_NOTES_UPDATE_PACKAGE with noteId - %s, message - %s",
                    update.getNoteId(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }
    }

    private void addDMSNotesUpdatePackageHistoryItem(Connection conn, long id, Timestamp modifiedAt, String status, String message) throws ServiceException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_NOTES_UPDATE_PACKAGE_HISTORY (ID, MODIFIED_AT, STATUS, MESSAGE) VALUES (?, ?, ?, ?)");
            ps.setLong(1, id);
            ps.setTimestamp(2, modifiedAt);
            ps.setString(3, status);
            ps.setString(4, message);
            ps.executeUpdate();
            logger.debug("addDMSNotesUpdatePackageHistoryItem - insert into SELMED_DMS_NOTES_UPDATE_PACKAGE_HISTORY with id - {} is successful", id);
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("addDMSNotesUpdatePackageHistoryItem: Error inserting in SELMED_DMS_NOTES_UPDATE_PACKAGE_HISTORY with id - %s, message - %s",
                    id, e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
    }

    public void createDMSCoveragesUpdatePackage(Connection conn, String operation, SelmedDMSCoveragesUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_COVERAGES_UPDATE_PACKAGE (ID, CREATED_AT, OPERATION, COVERAGE_ID, CREATION_DATE, MODIFICATION_DATE, LOAN_ID, NAME, QUOTA, FILE_NUMBER, AERAS_CAPPING_APPLIES, COVER_SUBMITTED, CURRENCY_CODE, INSURED_AMOUNT, MACAO_COVER_ID, STATUS) VALUES (nextval('selmed_dms_raf_update_package_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID");
            ps.setTimestamp(1, createdAt);
            ps.setString(2, operation);
            ps.setString(3, update.getCoverageId());
            if (update.getCreationDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(update.getCreationDate()));
            } else {
                ps.setNull(4, Types.TIMESTAMP);
            }

            if (update.getModificationDate() != null) {
                ps.setTimestamp(5, Timestamp.valueOf(update.getModificationDate()));
            } else {
                ps.setNull(5, Types.TIMESTAMP);
            }

            if (update.getLoanId() != null) {
                ps.setLong(6, update.getLoanId());
            } else {
                ps.setNull(6, Types.INTEGER);
            }

            if (update.getName() != null) {
                ps.setInt(7, update.getName());
            } else {
                ps.setNull(7, Types.INTEGER);
            }

            if (update.getQuota() != null) {
                ps.setDouble(8, update.getQuota());
            } else {
                ps.setNull(8, Types.NUMERIC);
            }

            if (update.getFileNumber() != null) {
                ps.setString(9, update.getFileNumber());
            } else {
                ps.setNull(9, Types.VARCHAR);
            }

            ps.setBoolean(10, update.getAerasCappingApplies());
            ps.setBoolean(11, update.getCoverSubmitted());

            if (update.getCurrencyCode() != null) {
                ps.setString(12, update.getCurrencyCode());
            } else {
                ps.setNull(12, Types.VARCHAR);
            }

            if (update.getInsuredAmount() != null) {
                ps.setLong(13, update.getInsuredAmount());
            } else {
                ps.setNull(13, Types.INTEGER);
            }

            if (update.getMacaoCoverId() != null) {
                ps.setString(14, update.getMacaoCoverId());
            } else {
                ps.setNull(14, Types.VARCHAR);
            }

            ps.setString(15, "ADDED");
            rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                logger.debug("createDMSCoveragesUpdatePackage - insert into SELMED_DMS_COVERAGES_UPDATE_PACKAGE with coverageId - {} is successful. New Id - {}",
                        update.getCoverageId(), id);
                this.addDMSCoveragesUpdatePackageHistoryItem(conn, id, createdAt, "ADDED", null);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("createDMSCoveragesUpdatePackage: Error inserting in SELMED_DMS_COVERAGES_UPDATE_PACKAGE with coverageId - %s, message - %s",
                    update.getCoverageId(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }
    }

    private void addDMSCoveragesUpdatePackageHistoryItem(Connection conn, long id, Timestamp modifiedAt, String status, String message) throws ServiceException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_COVERAGES_UPDATE_PACKAGE_HISTORY (ID, MODIFIED_AT, STATUS, MESSAGE) VALUES (?, ?, ?, ?)");
            ps.setLong(1, id);
            ps.setTimestamp(2, modifiedAt);
            ps.setString(3, status);
            ps.setString(4, message);
            ps.executeUpdate();
            logger.debug("addDMSCoveragesUpdatePackageHistoryItem - insert into SELMED_DMS_COVERAGES_UPDATE_PACKAGE_HISTORY with id - {} is successful", id);
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("addDMSCoveragesUpdatePackageHistoryItem: Error inserting in SELMED_DMS_COVERAGES_UPDATE_PACKAGE_HISTORY with id - %s, message - %s ",
                    id, e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
    }

    public void createDMSOptionsUpdatePackage(Connection conn, String operation, SelmedDMSOptionsUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_OPTIONS_UPDATE_PACKAGE (ID, CREATED_AT, OPERATION, OPTION_ID, CREATION_DATE, MODIFICATION_DATE, LOAN_ID, DESCRIPTION, FILE_NUMBER, NAME, AUTHOR_ID, IS_SELECTED, STATUS) VALUES (nextval('selmed_dms_raf_update_package_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID");
            ps.setTimestamp(1, createdAt);
            ps.setString(2, operation);
            ps.setString(3, update.getOptionId());
            if (update.getCreationDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(update.getCreationDate()));
            } else {
                ps.setNull(4, Types.TIMESTAMP);
            }

            if (update.getModificationDate() != null) {
                ps.setTimestamp(5, Timestamp.valueOf(update.getModificationDate()));
            } else {
                ps.setNull(5, Types.TIMESTAMP);
            }

            if (update.getLoanId() != null) {
                ps.setLong(6, update.getLoanId());
            } else {
                ps.setNull(6, Types.INTEGER);
            }

            if (update.getDescription() != null) {
                ps.setString(7, update.getDescription());
            } else {
                ps.setNull(7, Types.VARCHAR);
            }

            if (update.getFileNumber() != null) {
                ps.setString(8, update.getFileNumber());
            } else {
                ps.setNull(8, Types.VARCHAR);
            }

            if (update.getName() != null) {
                ps.setInt(9, update.getName());
            } else {
                ps.setNull(9, Types.INTEGER);
            }

            if (update.getAuthorId() != null) {
                ps.setString(10, update.getAuthorId());
            } else {
                ps.setNull(10, Types.VARCHAR);
            }

            if (update.isSelected() != null) {
                ps.setBoolean(11, update.isSelected());
            } else {
                ps.setNull(11, Types.VARCHAR);
            }

            ps.setString(12, "ADDED");
            rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                logger.debug("createDMSOptionsUpdatePackage - Insert into SELMED_DMS_OPTIONS_UPDATE_PACKAGE table with optionId-{} is successful. New Id - {}",
                        update.getOptionId(), id);
                this.addDMSOptionsUpdatePackageHistoryItem(conn, id, createdAt, "ADDED", null);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("createDMSOptionsUpdatePackage - Error processing insert into SELMED_DMS_OPTIONS_UPDATE_PACKAGE with optionId - %s, message - %s",
                    update.getOptionId(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }
    }

    private void addDMSOptionsUpdatePackageHistoryItem(Connection conn, long id, Timestamp modifiedAt, String status, String message) throws ServiceException {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("INSERT INTO SELMED_DMS_OPTIONS_UPDATE_PACKAGE_HISTORY (ID, MODIFIED_AT, STATUS, MESSAGE) VALUES (?, ?, ?, ?)");
            ps.setLong(1, id);
            ps.setTimestamp(2, modifiedAt);
            ps.setString(3, status);
            ps.setString(4, message);
            ps.executeUpdate();
            logger.debug("addDMSOptionsUpdatePackageHistoryItem - insert into SELMED_DMS_OPTIONS_UPDATE_PACKAGE_HISTORY with id - {} is successful", id);
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("addDMSOptionsUpdatePackageHistoryItem: Error inserting in SELMED_DMS_OPTIONS_UPDATE_PACKAGE_HISTORY with id - %s, message - %s",
                    id, e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
    }

    public long getGisCoverNumber(Connection conn) throws ServiceException {
        Statement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.createStatement();
            rs = ps.executeQuery("SELECT nextval('selmed_dms_gis_cover_number_seq')");
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getGisCoverNumber - Error retrieving gisCoverNumber, message - %s", e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
        }

        return 0;
    }

    public boolean processRAFUpdate(Connection conn, SelmedDMSRisksApplicationFileUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        try {
            switch (update.getOperation().toUpperCase()) {
                case "CREATE":
                    ps = conn.prepareStatement("INSERT INTO RisksApplicationFile (FileNumber, Id, CreationDate, ModificationDate, IsLocked, isArchived) VALUES (?, ?, ?, ?, ?, ?)");
                    ps.setString(1, update.getFileNumber());
                    ps.setString(2, UUID.randomUUID().toString());
                    ps.setTimestamp(3, Timestamp.valueOf(update.getCreationDate()));
                    ps.setTimestamp(4, Timestamp.valueOf(update.getModificationDate()));
                    ps.setBoolean(5, update.isLocked());
                    ps.setBoolean(6, update.isArchived());
                    if (ps.executeUpdate() == 1) {
                        logger.debug("processRAFUpdate - Insert into RisksApplicationFile table with fileNumber-{} is successful", update.getFileNumber());
                        return true;
                    }
                    break;
                case "MODIFY":
                    return updateSelmedDMSUpdatePackages(conn, update);
            }
        } catch (SQLException e) {
            logger.error("error: {}", e.getMessage());
            throw new ServiceException(String.format("processRAFUpdate - Error processing insert into RisksApplicationFile with fileNumber - %s, message - %s",
                    update.getFileNumber(), e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
        return false;
    }

    public boolean processCoverageUpdate(Connection conn, SelmedDMSCoveragesUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        try {
            switch (update.getOperation().toUpperCase()) {
                case "CREATE":
                    ps = conn.prepareStatement("INSERT INTO Coverages (Id, CreationDate, ModificationDate, LoanId, Name, Quota, ApplicationFileFileNumber, AerasCappingApplies, CoverSubmitted, CurrencyCode, InsuredAmount, MacaoCoverId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    ps.setString(1, update.getCoverageId());
                    ps.setTimestamp(2, Timestamp.valueOf(update.getCreationDate()));
                    ps.setTimestamp(3, Timestamp.valueOf(update.getModificationDate()));
                    ps.setLong(4, update.getLoanId());
                    ps.setInt(5, update.getName());
                    ps.setDouble(6, update.getQuota());
                    ps.setString(7, update.getFileNumber());
                    ps.setBoolean(8, update.getAerasCappingApplies());
                    ps.setBoolean(9, update.getCoverSubmitted());
                    ps.setString(10, update.getCurrencyCode());
                    ps.setLong(11, update.getInsuredAmount());
                    ps.setString(12, update.getMacaoCoverId());
                    if (ps.executeUpdate() == 1) {
                        logger.debug("processCoverageUpdate - Insert into Coverages with coverageId-{} is successful", update.getCoverageId());
                        return true;
                    }
                    break;
                case "MODIFY":
                    return updateSelmedDMSUpdatePackages(conn, update);
            }
        } catch (SQLException e) {
            logger.error("error {}", e.getMessage());
            throw new ServiceException(String.format("processCoverageUpdate - Error processing insert into Coverages with coverageId - %s, message - %s",
                    update.getCoverageId(), e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
        return false;
    }

    public boolean processOptionUpdate(Connection conn, SelmedDMSOptionsUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        try {
            switch (update.getOperation().toUpperCase()) {
                case "CREATE":
                    ps = conn.prepareStatement("INSERT INTO Options (Id, CreationDate, ModificationDate, LoanId, Description, ApplicationFileFileNumber, Name, AuthorId, IsSelected) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    ps.setString(1, update.getOptionId());
                    ps.setTimestamp(2, Timestamp.valueOf(update.getCreationDate()));
                    ps.setTimestamp(3, Timestamp.valueOf(update.getModificationDate()));
                    ps.setLong(4, update.getLoanId());
                    ps.setString(5, update.getDescription());
                    ps.setString(6, update.getFileNumber());
                    ps.setInt(7, update.getName());
                    ps.setString(8, update.getAuthorId());
                    ps.setBoolean(9, update.isSelected());

                    if (ps.executeUpdate() == 1) {
                        logger.debug("processOptionUpdate - Insert into Options table with optionsId-{} is successful", update.getOptionId());
                        return true;
                    }
                    break;
                case "MODIFY":
                    return updateSelmedDMSUpdatePackages(conn, update);
            }
        } catch (SQLException e) {
            logger.error("error: {}", e.getMessage());
            throw new ServiceException(String.format("processOptionUpdate - Error processing insert into Options with optionsId - %s, message - %s",
                    update.getOptionId(), e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
        return false;
    }

    public boolean processDAFUpdate(Connection conn, SelmedDMSDecisionsApplicationFileUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        try {
            switch (update.getOperation().toUpperCase()) {
                case "CREATE":
                    ps = conn.prepareStatement("INSERT INTO DecisionsApplicationFile (FileNumber, Id, CreationDate, ModificationDate, IsLocked, isArchived, HasMedicalRiskSheet) VALUES (?, ?, ?, ?, ?, ?, ?)");
                    ps.setString(1, update.getFileNumber());
                    ps.setString(2, UUID.randomUUID().toString());
                    ps.setTimestamp(3, Timestamp.valueOf(update.getCreationDate()));
                    ps.setTimestamp(4, Timestamp.valueOf(update.getModificationDate()));
                    ps.setBoolean(5, update.isLocked());
                    ps.setBoolean(6, update.isArchived());
                    ps.setBoolean(7, update.hasMedicalRiskSheet());
                    if (ps.executeUpdate() == 1) {
                        logger.debug("processDAFUpdate - Insert into DecisionsApplicationFile table with fileNumber-{} is successful", update.getFileNumber());
                        return true;
                    }
                    break;
                case "MODIFY":
                    return updateSelmedDMSUpdatePackages(conn, update);
            }
        } catch (SQLException e) {
            logger.error("error: {}", e.getMessage());
            throw new ServiceException(String.format("processDAFUpdate - Error processing insert into DecisionsApplicationFile with fileNumber - %s, message - %s",
                    update.getFileNumber(), e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
        return false;
    }

    public boolean processRisksUpdate(Connection conn, SelmedDMSRisksUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        try {
            switch (update.getOperation().toUpperCase()) {
                case "CREATE":
                    ps = conn.prepareStatement("INSERT INTO Risks (Id, CreationDate, ModificationDate, FileNumber, RiskType) VALUES (?, ?, ?, ?, ?)");
                    ps.setString(1, update.getRiskId());
                    ps.setTimestamp(2, Timestamp.valueOf(update.getCreationDate()));
                    ps.setTimestamp(3, Timestamp.valueOf(update.getModificationDate()));
                    ps.setString(4, update.getFileNumber());
                    ps.setInt(5, update.getRiskType());
                    if (ps.executeUpdate() == 1) {
                        logger.debug("processRisksUpdate - Insert into Risks table with riskId-{} is successful", update.getRiskId());
                        return true;
                    }
                    break;
                case "MODIFY":
                    return updateSelmedDMSUpdatePackages(conn, update);
            }
        } catch (SQLException e) {
            logger.error("error: {}", e.getMessage());
            throw new ServiceException(String.format("processRisksUpdate - Error processing insert into Risks with riskId - %s, message - %s",
                    update.getRiskId(), e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
        return false;
    }

    public boolean processNotesUpdate(Connection conn, SelmedDMSNotesUpdatePackage update) throws ServiceException {
        PreparedStatement ps = null;
        try {
            switch (update.getOperation().toUpperCase()) {
                case "CREATE":
                    ps = conn.prepareStatement("INSERT INTO Notes (Id, CreationDate, ModificationDate, Description, AuthorId, NoteType, RiskId) VALUES (?, ?, ?, ?, ?, ?, ?)");
                    ps.setString(1, update.getNoteId());
                    ps.setTimestamp(2, Timestamp.valueOf(update.getCreationDate()));
                    ps.setTimestamp(3, Timestamp.valueOf(update.getModificationDate()));
                    ps.setString(4, update.getDescription());
                    ps.setString(5, update.getAuthorId());
                    ps.setInt(6, update.getNoteType());
                    ps.setString(7, update.getRiskId());
                    if (ps.executeUpdate() == 1) {
                        logger.debug("processNotesUpdate - Insert into Notes table with noteId-{} is successful", update.getNoteId());
                        return true;
                    }
                    break;
                case "MODIFY":
                    return updateSelmedDMSUpdatePackages(conn, update);
            }
        } catch (SQLException e) {
            logger.error("error: {}", e.getMessage());
            throw new ServiceException(String.format("processNotesUpdate - Error processing insert into Notes with noteId - %s, message - %s",
                    update.getNoteId(), e.getMessage()));
        } finally {
            DBUtil.closeStatementQuietly(ps);
        }
        return false;
    }

    public Map<String, SelmedDMSRisksApplicationFileUpdatePackage> getRisksApplicationFilePackages() throws ServiceException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            st = conn.prepareStatement("SELECT id, created_at, operation, file_number, creation_date, modification_date, is_locked, is_archived, RAF_ID from selmed_dms_raf_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') ORDER BY ID LIMIT ?");
            st.setInt(1, this.applicationConfiguration.getIntegerProperty("dms.application.size", this.applicationSize));
            rs = st.executeQuery();
            Map<String, SelmedDMSRisksApplicationFileUpdatePackage> result = new HashMap<>();
            while (rs.next()) {
                SelmedDMSRisksApplicationFileUpdatePackage update = new SelmedDMSRisksApplicationFileUpdatePackage();
                update.setId(rs.getLong(1));
                update.setCreatedAt(rs.getTimestamp(2).toLocalDateTime());
                update.setOperation(rs.getString(3));
                update.setFileNumber(rs.getString(4));
                update.setCreationDate(rs.getTimestamp(5).toLocalDateTime());
                update.setModificationDate(rs.getTimestamp(6).toLocalDateTime());
                update.setLocked(rs.getBoolean(7));
                update.setArchived(rs.getBoolean(8));
                update.setApplicationId(rs.getString(9));
                result.put(update.getFileNumber(), update);
            }

            logger.debug("getRisksApplicationFilePackages - retrieved from selmed_dms_raf_update_package with result set size - {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getRisksApplicationFilePackages - Error selecing from selmed_dms_raf_update_package, message - %s", e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(st);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public Map<String, SelmedDMSDecisionsApplicationFileUpdatePackage> getDecisionsApplicationFilePackages() throws ServiceException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            st = conn.prepareStatement("SELECT id, created_at, operation, file_number, creation_date, modification_date, is_locked, is_archived, has_medical_risk_sheet from selmed_dms_daf_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') ORDER BY ID LIMIT ?");
            st.setInt(1, this.applicationConfiguration.getIntegerProperty("dms.application.size", this.applicationSize));
            rs = st.executeQuery();
            Map<String, SelmedDMSDecisionsApplicationFileUpdatePackage> result = new HashMap<>();
            while (rs.next()) {
                SelmedDMSDecisionsApplicationFileUpdatePackage update = new SelmedDMSDecisionsApplicationFileUpdatePackage();
                update.setId(rs.getLong(1));
                update.setCreatedAt(rs.getTimestamp(2).toLocalDateTime());
                update.setOperation(rs.getString(3));
                update.setFileNumber(rs.getString(4));
                update.setCreationDate(rs.getTimestamp(5).toLocalDateTime());
                update.setModificationDate(rs.getTimestamp(6).toLocalDateTime());
                update.setLocked(rs.getBoolean(7));
                update.setArchived(rs.getBoolean(8));
                update.setHasMedicalRiskSheet(rs.getBoolean(9));
                result.put(update.getFileNumber(), update);
            }
            logger.debug("getDecisionsApplicationFilePackages - retrieved from selmed_dms_daf_update_package with result set size - {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getDecisionsApplicationFilePackages - Error selecing from selmed_dms_daf_update_package, message - %s", e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(st);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public List<SelmedDMSNotesUpdatePackage> getNotesPackages() throws ServiceException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            st = conn.prepareStatement("SELECT id, created_at, operation, note_id, creation_date, modification_date, description, author_id, note_type, risk_id, file_number from selmed_dms_notes_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') ORDER BY ID LIMIT ?");
            st.setInt(1, this.applicationConfiguration.getIntegerProperty("dms.application.size", this.applicationSize));
            rs = st.executeQuery();
            List<SelmedDMSNotesUpdatePackage> result = new ArrayList<>();
            while (rs.next()) {
                SelmedDMSNotesUpdatePackage update = new SelmedDMSNotesUpdatePackage();
                update.setId(rs.getLong(1));
                update.setCreatedAt(rs.getTimestamp(2).toLocalDateTime());
                update.setOperation(rs.getString(3));
                update.setNoteId(rs.getString(4));
                update.setCreationDate(rs.getTimestamp(5).toLocalDateTime());
                update.setModificationDate(rs.getTimestamp(6).toLocalDateTime());
                update.setDescription(rs.getString(7));
                update.setAuthorId(rs.getString(8));
                update.setNoteType(rs.getInt(9));
                update.setRiskId(rs.getString(10));
                update.setFileNumber(rs.getString(11));
                result.add(update);
            }
            logger.debug("getNotesPackages - retrieved from selmed_dms_notes_update_package with result set size - {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getNotesPackages - Error selecing from selmed_dms_notes_update_package, message - %s", e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(st);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void updateRisksApplicationFilePackageStatus(long id, String status, LocalDateTime modifiedAt, String message) throws ServiceException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = this.dataSource.getConnection();
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("UPDATE selmed_dms_raf_update_package SET status = ?, last_updated_at = ?, message = ? where id = ?");
            ps.setString(1, status);
            ps.setTimestamp(2, Timestamp.valueOf(modifiedAt));
            ps.setString(3, message);
            ps.setLong(4, id);
            if (ps.executeUpdate() == 1) {
                logger.debug("updateRisksApplicationFilePackageStatus - Update selmed_dms_raf_update_package to - {} for risk application file package with id - {} is successful", status, id);
                this.addDMSRisksApplicationFileUpdatePackageHistoryItem(conn, id, Timestamp.valueOf(modifiedAt), status, message);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "updateRisksApplicationFilePackageStatus - Error updating selmed_dms_raf_update_package to status - %s for id - $s, message - %s",
                    status, id, e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void updateDecisionsApplicationFilePackageStatus(long id, String status, LocalDateTime modifiedAt, String message) throws ServiceException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = this.dataSource.getConnection();
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("UPDATE selmed_dms_daf_update_package SET status = ?, last_updated_at = ?, message = ? where id = ?");
            ps.setString(1, status);
            ps.setTimestamp(2, Timestamp.valueOf(modifiedAt));
            ps.setString(3, message);
            ps.setLong(4, id);
            if (ps.executeUpdate() == 1) {
                logger.debug("updateDecisionsApplicationFilePackageStatus - Update selmed_dms_daf_update_package to - {} for daf package with id - {} is successful", status, id);
                this.addDMSDecisionsApplicationFileUpdatePackageHistoryItem(conn, id, Timestamp.valueOf(modifiedAt), status, message);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "updateDecisionsApplicationFilePackageStatus - Error updating selmed_dms_daf_update_package to status - %s for id - $s, message - %s",
                    status, id, e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void updateCoveragesPackageStatus(long id, String status, LocalDateTime modifiedAt, String message) throws ServiceException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = this.dataSource.getConnection();
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("UPDATE selmed_dms_coverages_update_package SET status = ?, last_updated_at = ?, message = ? where id = ?");
            ps.setString(1, status);
            ps.setTimestamp(2, Timestamp.valueOf(modifiedAt));
            ps.setString(3, message);
            ps.setLong(4, id);
            if (ps.executeUpdate() == 1) {
                logger.debug("updateCoveragesPackageStatus - Update selmed_dms_coverages_update_package to - {} for coverage package with id - {} is successful",
                        status, id);
                this.addDMSCoveragesUpdatePackageHistoryItem(conn, id, Timestamp.valueOf(modifiedAt), status, message);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "updateCoveragesPackageStatus - Error updating selmed_dms_coverages_update_package to status - %s for id - $s, message - %s",
                    status, id, e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void updateOptionsPackageStatus(long id, String status, LocalDateTime modifiedAt, String message) throws ServiceException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = this.dataSource.getConnection();
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("UPDATE selmed_dms_options_update_package SET status = ?, last_updated_at = ?, message = ? where id = ?");
            ps.setString(1, status);
            ps.setTimestamp(2, Timestamp.valueOf(modifiedAt));
            ps.setString(3, message);
            ps.setLong(4, id);
            if (ps.executeUpdate() == 1) {
                logger.debug("updateOptionsPackageStatus - Update selmed_dms_options_update_package to - {} for options package with id - {} is successful",
                        status, id);
                this.addDMSOptionsUpdatePackageHistoryItem(conn, id, Timestamp.valueOf(modifiedAt), status, message);
            }
        } catch (SQLException ex) {
            logger.error("error", ex);
            throw new ServiceException(String.format(
                    "updateOptionsPackageStatus - Error updating selmed_dms_options_update_package to status - %s for id - $s, message - %s",
                    status, id, ex.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void updateRisksPackageStatus(long id, String status, LocalDateTime modifiedAt, String message) throws ServiceException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = this.dataSource.getConnection();
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("UPDATE selmed_dms_risks_update_package SET status = ?, last_updated_at = ?, message = ? where id = ?");
            ps.setString(1, status);
            ps.setTimestamp(2, Timestamp.valueOf(modifiedAt));
            ps.setString(3, message);
            ps.setLong(4, id);
            if (ps.executeUpdate() == 1) {
                logger.debug("updateRisksPackageStatus - Update selmed_dms_risks_update_package to - {} for risks package with id - {} is successful",
                        status, id);
                this.addDMSRisksUpdatePackageHistoryItem(conn, id, Timestamp.valueOf(modifiedAt), status, message);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "updateRisksPackageStatus - error updating selmed_dms_risks_update_package to status - %s for id - $s, message - %s",
                    status, id, e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void updateNotesPackageStatus(long id, String status, LocalDateTime modifiedAt, String message) throws ServiceException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = this.dataSource.getConnection();
            Timestamp createdAt = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
            ps = conn.prepareStatement("UPDATE selmed_dms_notes_update_package SET status = ?, last_updated_at = ?, message = ? where id = ?");
            ps.setString(1, status);
            ps.setTimestamp(2, Timestamp.valueOf(modifiedAt));
            ps.setString(3, message);
            ps.setLong(4, id);
            if (ps.executeUpdate() == 1) {
                logger.debug("updateNotesPackageStatus - Update selmed_dms_notes_update_package to - {} for notes package with id - {} is successful",
                        status, id);
                this.addDMSNotesUpdatePackageHistoryItem(conn, id, Timestamp.valueOf(modifiedAt), status, message);
            }
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format(
                    "updateNotesPackageStatus - Error updating selmed_dms_notes_update_package to status - %s for id - $s, message - %s",
                    status, id, e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public List<SelmedDMSRisksUpdatePackage> getRisksPackages() throws ServiceException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            st = conn.prepareStatement("SELECT id, created_at, operation, risk_id, creation_date, modification_date, risk_type, file_number from selmed_dms_risks_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') ORDER BY ID LIMIT ?");
            st.setInt(1, this.applicationConfiguration.getIntegerProperty("dms.application.size", this.applicationSize));
            rs = st.executeQuery();
            List<SelmedDMSRisksUpdatePackage> result = new ArrayList<>();
            while (rs.next()) {
                SelmedDMSRisksUpdatePackage update = new SelmedDMSRisksUpdatePackage();
                update.setId(rs.getLong(1));
                update.setCreatedAt(rs.getTimestamp(2).toLocalDateTime());
                update.setOperation(rs.getString(3));
                update.setRiskId(rs.getString(4));
                update.setCreationDate(rs.getTimestamp(5).toLocalDateTime());
                update.setModificationDate(rs.getTimestamp(6).toLocalDateTime());
                update.setRiskType(rs.getInt(7));
                update.setFileNumber(rs.getString(8));
                result.add(update);
            }

            logger.debug("getRisksPackages - retrieved risk packages with result set size - {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getRisksPackages - Error selecing from selmed_dms_risks_update_package, message - %s", e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(st);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public List<SelmedDMSCoveragesUpdatePackage> getCoveragesPackages() throws ServiceException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            st = conn.prepareStatement("SELECT id, created_at, operation, coverage_id, creation_date, modification_date, loan_id, name, quota, file_number, aeras_capping_applies, cover_submitted, currency_code, insured_amount, macao_cover_id from selmed_dms_coverages_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') AND file_number in (select file_number from (SELECT id, file_number from selmed_dms_daf_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') ORDER BY ID LIMIT ?) source)");
            st.setInt(1, this.applicationConfiguration.getIntegerProperty("dms.coverages.size", this.coveragesSize));
            rs = st.executeQuery();
            List<SelmedDMSCoveragesUpdatePackage> result = new ArrayList<>();
            while (rs.next()) {
                SelmedDMSCoveragesUpdatePackage update = new SelmedDMSCoveragesUpdatePackage();
                update.setId(rs.getLong(1));
                update.setCreatedAt(rs.getTimestamp(2).toLocalDateTime());
                update.setOperation(rs.getString(3));
                update.setCoverageId(rs.getString(4));
                update.setCreationDate(rs.getTimestamp(5).toLocalDateTime());
                update.setModificationDate(rs.getTimestamp(6).toLocalDateTime());
                update.setLoanId(rs.getLong(7));
                update.setName(rs.getInt(8));
                update.setQuota(rs.getDouble(9));
                update.setFileNumber(rs.getString(10));
                update.setAerasCappingApplies(rs.getBoolean(11));
                update.setCoverSubmitted(rs.getBoolean(12));
                update.setCurrencyCode(rs.getString(13));
                update.setInsuredAmount(rs.getLong(14));
                update.setMacaoCoverId(rs.getString(15));
                result.add(update);
            }
            logger.debug("getCoveragesPackages - retrieved ceoverage packages with result set size - {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getCoveragesPackages - Error selecing from selmed_dms_coverages_update_package, message - %s", e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(st);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public List<SelmedDMSOptionsUpdatePackage> getOptionsPackages() throws ServiceException {
        Connection conn = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            conn = this.dataSource.getConnection();
            st = conn.prepareStatement("SELECT id, created_at, operation, option_id, creation_date, modification_date, loan_id, description, file_number, name, author_id, is_selected from selmed_dms_options_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') AND file_number in (select file_number from (SELECT id, file_number from selmed_dms_daf_update_package where status = 'ADDED' AND operation in ('CREATE', 'MODIFY') ORDER BY ID LIMIT ?) source)");
            st.setInt(1, this.applicationConfiguration.getIntegerProperty("dms.application.size", this.applicationSize));
            rs = st.executeQuery();
            List<SelmedDMSOptionsUpdatePackage> result = new ArrayList<>();
            while (rs.next()) {
                SelmedDMSOptionsUpdatePackage update = new SelmedDMSOptionsUpdatePackage();
                update.setId(rs.getLong(1));
                update.setCreatedAt(rs.getTimestamp(2).toLocalDateTime());
                update.setOperation(rs.getString(3));
                update.setOptionId(rs.getString(4));
                update.setCreationDate(rs.getTimestamp(5).toLocalDateTime());
                update.setModificationDate(rs.getTimestamp(6).toLocalDateTime());
                update.setLoanId(rs.getLong(7));
                update.setDescription(rs.getString(8));
                update.setFileNumber(rs.getString(9));
                update.setName(rs.getInt(10));
                update.setAuthorId(rs.getString(11));
                update.setSelected(rs.getBoolean(12));
                result.add(update);
            }
            logger.debug("getOptionsPackages - retrieved options packages with result set size - {}", result.size());
            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getOptionsPackages - Error selecing from selmed_dms_options_update_package, message - %s",
                    e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(st);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    boolean updateSelmedDMSUpdatePackages(Connection conn, Entity updateEntity) throws ServiceException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int status = 0;
        try {
            ps = conn.prepareStatement(prepareUpdateStatement(updateEntity));
            ps = populatePreparedStatement(ps, updateEntity.getFields());
            status = ps.executeUpdate();
            logger.debug("updateSelmedUpdatePackages, status after update - {}", status);
        } catch (SQLException e) {
            logger.error("error: {}", e.getMessage());
            throw new ServiceException(String.format("updateSelmedUpdatePackages - error updating Update package with table name - %s, error - %s",
                    updateEntity.getTableName(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rs);
            DBUtil.closeStatementQuietly(ps);
            DBUtil.closeConnectionQuietly(conn);
        }
        return status == 1;
    }

    private PreparedStatement populatePreparedStatement(PreparedStatement ps, List<Field> fields) throws ServiceException {
        for (Iterator<Field> iterator = fields.iterator(); iterator.hasNext(); ) {
            Field field = (Field) iterator.next();
            if (iterator.hasNext()) {
                try {
                    switch (field.getType()) {
                        case STRING -> {
                            ps.setString(field.getIndex(), String.valueOf(field.getValue()));
                            logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), field.getValue());
                            break;
                        }
                        case DATE, TIMESTAMP -> {
                            if (field.getValue() instanceof LocalDateTime) {
                                LocalDate date = ((LocalDateTime) field.getValue()).toLocalDate();
                                ps.setDate(field.getIndex(), java.sql.Date.valueOf(date));
                                logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), java.sql.Date.valueOf(date));
                            } else if (field.getValue() instanceof LocalDate) {
                                ps.setDate(field.getIndex(), java.sql.Date.valueOf((LocalDate) field.getValue()));
                                logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), java.sql.Date.valueOf((LocalDate) field.getValue()));
                            }
                            break;
                        }
                        case INTEGER -> {
                            ps.setInt(field.getIndex(), Integer.parseInt(String.valueOf(field.getValue())));
                            logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), Integer.valueOf(String.valueOf(field.getValue())));
                            break;
                        }
                        case BOOLEAN -> {
                            if (field.getValue() instanceof Boolean) {
                                ps.setBoolean(field.getIndex(), Boolean.parseBoolean(String.valueOf(field.getValue())));
                                logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), Boolean.valueOf(String.valueOf(field.getValue())));
                            } else {
                                logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), String.valueOf(field.getValue()));
                                ps.setString(field.getIndex(), String.valueOf(field.getValue()));
                            }
                            break;
                        }
                        case LONG -> {
                            logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), Long.valueOf(String.valueOf(field.getValue())));
                            ps.setLong(field.getIndex(), Long.parseLong(String.valueOf(field.getValue())));
                            break;
                        }
                        case DOUBLE -> {
                            logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), Double.valueOf(String.valueOf(field.getValue())));
                            ps.setDouble(field.getIndex(), Double.parseDouble(String.valueOf(field.getValue())));
                            break;
                        }
                        default -> {
                            logger.debug("Setting parameter at index - {} to - {}", field.getIndex(), String.valueOf(field.getValue()));
                            ps.setString(field.getIndex(), String.valueOf(field.getValue()));
                        }
                    }
                } catch (SQLException ex) {
                    logger.error("error", ex);
                    throw new ServiceException(String.format("populatePreparedStatement - Error populating fields in prepared statement, message - %s",
                            ex.getMessage()));
                }
            }
        }
        return ps;
    }

    private String prepareUpdateStatement(Entity entityToBeUpdated) {
        StringBuilder queryBuilder = new StringBuilder();

        if (entityToBeUpdated.getFields() != null && !entityToBeUpdated.getFields().isEmpty()) {
            queryBuilder.append(String.format("UPDATE %s SET ", entityToBeUpdated.getTableName()));
            List<String> columnsToBeUpdated = new ArrayList<String>();
            Iterator<Field> fieldsIterator = entityToBeUpdated.getFields().iterator();

            while (fieldsIterator.hasNext()) {
                Field field = fieldsIterator.next();
                if (!fieldsIterator.hasNext()) {
                    queryBuilder.append(String.format("%s where %s='%s'", String.join(", ", columnsToBeUpdated),
                            field.getName(), String.valueOf(field.getValue())));
                } else {
                    columnsToBeUpdated.add(String.format("%s=?", field.getName()));
                }
            }
        }
        logger.debug("prepareUpdateStatement, prepared query - {}", queryBuilder);
        return queryBuilder.toString();
    }

    public void updateLoanId(LoanIds loan) throws ServiceException {
        Connection dbMigrationConnection = null;
        Connection dmsDecisionsConnection = null;
        PreparedStatement dbMigrationCoveragesStatement = null;
        PreparedStatement dbMigrationGisCoveragesStatement = null;
        PreparedStatement dbMigrationOptionsStatement = null;
        PreparedStatement dmsUpdateCoveragesStatement = null;
        PreparedStatement dmsUpdateOptionsStatement = null;
        PreparedStatement dmsInsertMapEntry = null;
        PreparedStatement dmsCheckMapEntry = null;
        ResultSet dmsCheckMapEntryResultSet = null;

        try {
            String loanId = Long.toString(loan.getMacaoLoanId());
            dmsDecisionsConnection = this.decisionsDataSource.getConnection();
            dbMigrationConnection = this.dataSource.getConnection();

            dmsCheckMapEntry = dmsDecisionsConnection.prepareStatement("SELECT LoanID FROM RefLoanIDMacoaIDLoanMap WHERE MacoaLoanID = ?");
            dmsInsertMapEntry = dmsDecisionsConnection.prepareStatement("INSERT INTO RefLoanIDMacoaIDLoanMap (LoanID, MacoaLoanID) VALUES (?, ?)");

            logger.info("Checking if map entry exists for {}", loanId);
            dmsCheckMapEntry.setString(1, loanId);
            dmsCheckMapEntryResultSet = dmsCheckMapEntry.executeQuery();
            if (!dmsCheckMapEntryResultSet.next()) {
                dmsInsertMapEntry.setString(1, loan.getSalesforceLoanId());
                dmsInsertMapEntry.setString(2, loanId);
                logger.info("Inserting pair {} - {}", loan.getSalesforceLoanId(), loanId);
                dmsInsertMapEntry.executeUpdate();
            }

            logger.info("Updating DMS coverages with '{}'", loan.getSalesforceLoanId());
            dmsUpdateCoveragesStatement = dmsDecisionsConnection.prepareStatement("UPDATE Coverages SET LoanId = ? WHERE LoanId = ?");
            dmsUpdateCoveragesStatement.setString(1, loan.getSalesforceLoanId());
            dmsUpdateCoveragesStatement.setString(2, loanId);
            dmsUpdateCoveragesStatement.executeUpdate();

            logger.info("Updating DMS Options with '{}'", loan.getSalesforceLoanId());
            dmsUpdateOptionsStatement = dmsDecisionsConnection.prepareStatement("UPDATE Options SET LoanId = ? WHERE LoanId = ?");
            dmsUpdateOptionsStatement.setString(1, loan.getSalesforceLoanId());
            dmsUpdateOptionsStatement.setString(2, loanId);
            dmsUpdateOptionsStatement.executeUpdate();

            logger.info("Updating migration coverages with '{}'", loan.getSalesforceLoanId());
            dbMigrationCoveragesStatement = dbMigrationConnection.prepareStatement("UPDATE selmed_dms_coverages SET sf_loan_id = ? where loan_id = ?");
            dbMigrationCoveragesStatement.setString(1, loan.getSalesforceLoanId());
            dbMigrationCoveragesStatement.setLong(2, loan.getMacaoLoanId());
            dbMigrationCoveragesStatement.executeUpdate();

            logger.info("Updating migration GIS coverages with '{}'", loan.getSalesforceLoanId());
            dbMigrationGisCoveragesStatement = dbMigrationConnection.prepareStatement("UPDATE selmed_dms_gis_coverages SET sf_loan_id = ? where loan_id = ?");
            dbMigrationGisCoveragesStatement.setString(1, loan.getSalesforceLoanId());
            dbMigrationGisCoveragesStatement.setLong(2, loan.getMacaoLoanId());
            dbMigrationGisCoveragesStatement.executeUpdate();

            logger.info("Updating migration options with '{}'", loan.getSalesforceLoanId());
            dbMigrationOptionsStatement = dbMigrationConnection.prepareStatement("UPDATE selmed_dms_options SET sf_loan_id = ? where loan_id = ?");
            dbMigrationOptionsStatement.setString(1, loan.getSalesforceLoanId());
            dbMigrationOptionsStatement.setLong(2, loan.getMacaoLoanId());
            dbMigrationOptionsStatement.executeUpdate();

            logger.info("Updated Macao Loan Id {} in DMS with the Salesforce Loan Id '{}'", loanId, loan.getSalesforceLoanId());
        } catch (Exception e) {
            logger.error("Cannot update loan Id {} in DMS Decisions: {}", loan.getMacaoLoanId(), e.getMessage());
            throw new ServiceException(String.format("Failed to update loan Id %dl with '%s': %s", loan.getMacaoLoanId(), loan.getSalesforceLoanId(), e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(dmsCheckMapEntryResultSet);
            DBUtil.closeStatementQuietly(dmsCheckMapEntry);
            DBUtil.closeStatementQuietly(dmsInsertMapEntry);
            DBUtil.closeStatementQuietly(dmsUpdateOptionsStatement);
            DBUtil.closeStatementQuietly(dmsUpdateCoveragesStatement);
            DBUtil.closeStatementQuietly(dbMigrationOptionsStatement);
            DBUtil.closeStatementQuietly(dbMigrationGisCoveragesStatement);
            DBUtil.closeStatementQuietly(dbMigrationCoveragesStatement);
            DBUtil.closeConnectionQuietly(dmsDecisionsConnection);
            DBUtil.closeConnectionQuietly(dbMigrationConnection);
        }
    }

    public DMSStatistics getStatistics() {
        Connection conn = null;
        Statement stStatistics = null;
        ResultSet rsStatistics = null;
        ResultSet rsObjectStatistics = null;
        DMSStatistics result = new DMSStatistics();

        try {
            conn = this.dataSource.getConnection();

            stStatistics = conn.createStatement();
            rsStatistics = stStatistics.executeQuery("""
                select 'RAF_' || status status, count(*) from selmed_dms_raf_update_package where status in ('ADDED', 'PROCESSED', 'ERROR') group by status
                UNION ALL
                select 'RISKS_' || status status, count(*) from selmed_dms_risks_update_package where status in ('ADDED', 'PROCESSED', 'ERROR') group by status
                UNION ALL
                select 'NOTES_' || status status, count(*) from selmed_dms_notes_update_package where status in ('ADDED', 'PROCESSED', 'ERROR') group by status
                UNION ALL
                select 'DAF_' || status status, count(*) from selmed_dms_daf_update_package where status in ('ADDED', 'PROCESSED', 'ERROR') group by status
                UNION ALL
                select 'COVERAGES_' || status status, count(*) from selmed_dms_coverages_update_package where status in ('ADDED', 'PROCESSED', 'ERROR') group by status
                UNION ALL
                select 'OPTIONS_' || status status, count(*) from selmed_dms_options_update_package where status in ('ADDED', 'PROCESSED', 'ERROR') group by status
                UNION ALL
                select 'DECISIONS_' || status status, count(*) from selmed_dms_decisions where status in ('CREATED', 'IGNORED', 'COMPLETED', 'ERROR') group by status            
            """);
            while (rsStatistics.next()) {
                switch (rsStatistics.getString(1)) {
                    case "RAF_ADDED" -> result.setRisksApplicationFilesAdded(rsStatistics.getInt(2));
                    case "RAF_PROCESSED" -> result.setRisksApplicationFilesProcessed(rsStatistics.getInt(2));
                    case "RAF_ERROR" -> result.setRisksApplicationFilesError(rsStatistics.getInt(2));
                    case "RISKS_ADDED" -> result.setRisksAdded(rsStatistics.getInt(2));
                    case "RISKS_PROCESSED" -> result.setRisksProcessed(rsStatistics.getInt(2));
                    case "RISKS_ERROR" -> result.setRisksError(rsStatistics.getInt(2));
                    case "NOTES_ADDED" -> result.setNotesAdded(rsStatistics.getInt(2));
                    case "NOTES_PROCESSED" -> result.setNotesProcessed(rsStatistics.getInt(2));
                    case "NOTES_ERROR" -> result.setNotesError(rsStatistics.getInt(2));
                    case "DAF_ADDED" -> result.setDecisionApplicationFilesAdded(rsStatistics.getInt(2));
                    case "DAF_PROCESSED" -> result.setDecisionApplicationFilesProcessed(rsStatistics.getInt(2));
                    case "DAF_ERROR" -> result.setDecisionApplicationFilesError(rsStatistics.getInt(2));
                    case "COVERAGES_ADDED" -> result.setCoveragesAdded(rsStatistics.getInt(2));
                    case "COVERAGES_PROCESSED" -> result.setCoveragesProcessed(rsStatistics.getInt(2));
                    case "COVERAGES_ERROR" -> result.setCoveragesError(rsStatistics.getInt(2));
                    case "OPTIONS_ADDED" -> result.setOptionsAdded(rsStatistics.getInt(2));
                    case "OPTIONS_PROCESSED" -> result.setOptionsProcessed(rsStatistics.getInt(2));
                    case "OPTIONS_ERROR" -> result.setOptionsError(rsStatistics.getInt(2));
                    case "DECISIONS_CREATED" -> result.setDecisionsCreated(rsStatistics.getInt(2));
                    case "DECISIONS_IGNORED" -> result.setDecisionsIgnored(rsStatistics.getInt(2));
                    case "DECISIONS_COMPLETED" -> result.setDecisionsCompleted(rsStatistics.getInt(2));
                    case "DECISIONS_ERROR" -> result.setDecisionsError(rsStatistics.getInt(2));
                }
            }
            rsObjectStatistics = stStatistics.executeQuery("""
                select 'APPLICATIONS', count(*) from selmed_dms_applications
                union all
                select 'COVERAGES', count(*) from selmed_dms_coverages
                union all
                select 'GIS_COVERAGES', count(*) from selmed_dms_gis_coverages
                union all
                select 'OPTIONS', count(*) from selmed_dms_options
            """);
            while(rsObjectStatistics.next()) {
                switch (rsObjectStatistics.getString(1)) {
                    case "APPLICATIONS" -> result.setApplicationsCreated(rsObjectStatistics.getInt(2));
                    case "COVERAGES" -> result.setCoveragesCreated(rsObjectStatistics.getInt(2));
                    case "GIS_COVERAGES" -> result.setGisCoveragesCreated(rsObjectStatistics.getInt(2));
                    case "OPTIONS" -> result.setOptionsCreated(rsObjectStatistics.getInt(2));
                }
            }
        } catch (SQLException e) {
            logger.error("Cannot retrieve SF update statistics: " + e.getMessage());
        } finally {
            closeResultSetQuietly(rsObjectStatistics);
            closeResultSetQuietly(rsStatistics);
            closeStatementQuietly(stStatistics);
            closeConnectionQuietly(conn);
        }

        return result;
    }

    public void storeDecisionData(Connection conn, DMSDecisionData data, LocalDateTime now) throws DecisionProcessingException {
        PreparedStatement stCreateDecisionRecord = null;
        PreparedStatement stCreateCoveragePremiumRecord = null;
        PreparedStatement stCreateCoverageExclusionRecord = null;
        PreparedStatement stCreateAdjournmentRecord = null;
        PreparedStatement stUpdateDecisionAdjournment = null;
        EntityType entityType = null;
        long entityId = -1;
        long decisionId = data.getId();

        try {
            stCreateDecisionRecord = conn.prepareStatement("""
                INSERT INTO Decisions (
                      ID
                    , CreationDate
                    , ModificationDate
                    , RiskType
                    , DecisionStatus
                    , HasContractualExclusion
                    , HasExtraPremium
                    , HasPartialExclusion
                    , LoanId
                    , CoverageId
                    , OptionId
                    , DecisionType
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """);

            stCreateCoveragePremiumRecord = conn.prepareStatement("""
                INSERT INTO CoveragePremiums (
                      ID
                    , CreationDate
                    , ModificationDate
                    , LinkType
                    , PremiumType
                    , Rate
                    , Unit
                    , Duration
                    , DecisionId   
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """);

            stCreateCoverageExclusionRecord = conn.prepareStatement("""
                INSERT INTO CoverageExclusions (
                      ID
                    , CreationDate
                    , ModificationDate
                    , ExclusionType
                    , Code
                    , Description
                    , DecisionId
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """);

            stCreateAdjournmentRecord = conn.prepareStatement("""
                INSERT INTO Adjournments (
                      ID
                    , CreationDate
                    , ModificationDate
                    , AdjournmentType
                    , AdjournmentDetail
                ) VALUES (?, ?, ?, ?, ?)
            """);

            stUpdateDecisionAdjournment = conn.prepareStatement("""
                UPDATE DECISIONS SET ADJOURNMENTID = ? WHERE ID = ?
            """);

            stCreateDecisionRecord.setString(1, data.getDecisionId());
            stCreateDecisionRecord.setTimestamp(2, Timestamp.valueOf(data.getCreationDate()));
            stCreateDecisionRecord.setTimestamp(3, Timestamp.valueOf(now));
            stCreateDecisionRecord.setInt(4, data.getRiskType());
            stCreateDecisionRecord.setInt(5, data.getDecisionStatus());
            stCreateDecisionRecord.setInt(6, data.getHasContractualExclusion());
            stCreateDecisionRecord.setInt(7, data.getHasExtraPremium());
            stCreateDecisionRecord.setInt(8, data.getHasPartialExclusion());
            stCreateDecisionRecord.setString(9, data.getLoanId());
            stCreateDecisionRecord.setString(10, data.getCoverageId());
            stCreateDecisionRecord.setString(11, data.getOptionId());
            stCreateDecisionRecord.setInt(12, data.getDecisionType());
            stCreateDecisionRecord.executeUpdate();

            if (data.getAdjournment() != null) {
                Adjournment adjournment = data.getAdjournment();
                entityType = EntityType.ADJOURNMENT;
                entityId = adjournment.getMigrationId();

                stCreateAdjournmentRecord.setString(1, adjournment.getId());
                stCreateAdjournmentRecord.setTimestamp(2, Timestamp.valueOf(data.getCreationDate()));
                stCreateAdjournmentRecord.setTimestamp(3, Timestamp.valueOf(now));
                stCreateAdjournmentRecord.setInt(4, adjournment.getType());
                stCreateAdjournmentRecord.setString(5, adjournment.getDetails());
                stCreateAdjournmentRecord.executeUpdate();

                stUpdateDecisionAdjournment.setString(1, adjournment.getId());
                stUpdateDecisionAdjournment.setString(2, data.getDecisionId());
                stUpdateDecisionAdjournment.executeUpdate();
            }

            if (data.getCoveragePremium() != null) {
                CoveragePremium premium = data.getCoveragePremium();
                entityType = EntityType.PREMIUM;
                entityId = premium.getId();

                stCreateCoveragePremiumRecord.setString(1, premium.getCoverPremiumId());
                stCreateCoveragePremiumRecord.setTimestamp(2, Timestamp.valueOf(data.getCreationDate()));
                stCreateCoveragePremiumRecord.setTimestamp(3, Timestamp.valueOf(now));
                stCreateCoveragePremiumRecord.setInt(4, premium.getLinkType());
                stCreateCoveragePremiumRecord.setInt(5, premium.getPremiumType());
                stCreateCoveragePremiumRecord.setInt(6, premium.getPremiumRate());
                stCreateCoveragePremiumRecord.setInt(7, premium.getUnit());
                stCreateCoveragePremiumRecord.setInt(8, premium.getDuration());
                stCreateCoveragePremiumRecord.setString(9, data.getDecisionId());
                stCreateCoveragePremiumRecord.executeUpdate();
            }

            if (data.getExclusions() != null) {
                for (Exclusion exclusion : data.getExclusions()) {
                    entityType = EntityType.EXCLUSION;
                    entityId = exclusion.getId();

                    stCreateCoverageExclusionRecord.setString(1, exclusion.getCoverExclusionId());
                    stCreateCoverageExclusionRecord.setTimestamp(2, Timestamp.valueOf(data.getCreationDate()));
                    stCreateCoverageExclusionRecord.setTimestamp(3, Timestamp.valueOf(now));
                    stCreateCoverageExclusionRecord.setInt(4, exclusion.getType());
                    stCreateCoverageExclusionRecord.setString(5, exclusion.getCode());
                    stCreateCoverageExclusionRecord.setString(6, exclusion.getDescription());
                    stCreateCoverageExclusionRecord.setString(7, data.getDecisionId());
                    stCreateCoverageExclusionRecord.executeUpdate();
                }
            }
        } catch (SQLException e) {
            logger.error(String.format("Cannot store DMS Decision data: %s", e.getMessage()));
            if (entityId > -1) {
                String msg = String.format("Failed to create entity %d: %s", entityId, e.getMessage());
                DecisionProcessingException exception = new DecisionProcessingException(entityType, entityId, msg);
                msg = String.format("Dependant object creation failure. Entity Type: %s, Entity Id: %d", entityType, entityId);
                exception.add(EntityType.DECISION, decisionId, msg);
                throw  exception;
            } else {
                String msg = String.format("Failed to create decision %d: %s", decisionId, e.getMessage());
                throw new DecisionProcessingException(EntityType.DECISION, decisionId, msg);
            }
        } finally {
            DBUtil.closeStatementQuietly(stUpdateDecisionAdjournment);
            DBUtil.closeStatementQuietly(stCreateAdjournmentRecord);
            DBUtil.closeStatementQuietly(stCreateCoverageExclusionRecord);
            DBUtil.closeStatementQuietly(stCreateCoveragePremiumRecord);
            DBUtil.closeStatementQuietly(stCreateDecisionRecord);
        }
    }
    public void rollbackDecision(DMSDecisionData data) throws ServiceException {
        Connection conn = null;
        PreparedStatement stDeleteAdjournmentRecord = null;
        PreparedStatement stDeleteCoverageExclusionRecord = null;
        PreparedStatement stDeleteCoveragePremiumRecord = null;
        PreparedStatement stDeleteDecisionRecord = null;

        try {
            conn = this.decisionsDataSource.getConnection();
            conn.setAutoCommit(false);
            stDeleteAdjournmentRecord = conn.prepareStatement("DELETE FROM Adjournments WHERE ID = ?");
            stDeleteCoverageExclusionRecord = conn.prepareStatement("DELETE FROM CoverageExclusions WHERE ID = ?");
            stDeleteCoveragePremiumRecord = conn.prepareStatement("DELETE FROM CoveragePremiums WHERE ID = ?");
            stDeleteDecisionRecord = conn.prepareStatement("DELETE FROM Decisions WHERE ID = ?");

            if (data.getAdjournment() != null) {
                stDeleteAdjournmentRecord.setString(1, data.getAdjournment().getId());
                stDeleteAdjournmentRecord.executeUpdate();
            }

            if (data.getExclusions() != null) {
                for (Exclusion exclusion : data.getExclusions()) {
                    stDeleteCoverageExclusionRecord.setString(1, exclusion.getCoverExclusionId());
                    stDeleteCoverageExclusionRecord.executeUpdate();
                }
            }

            if (data.getCoveragePremium() != null) {
                stDeleteCoveragePremiumRecord.setString(1, data.getCoveragePremium().getCoverPremiumId());
                stDeleteCoveragePremiumRecord.executeUpdate();
            }

            stDeleteDecisionRecord.setString(1, data.getDecisionId());
            stDeleteDecisionRecord.executeUpdate();
            DBUtil.commit(conn);
        } catch (SQLException e) {
            String message = String.format("Cannot delete DMS Decision data: %s", e.getMessage());
            logger.error(message);
            DBUtil.rollback(conn);
            throw new ServiceException(message);
        } finally {
            DBUtil.closeStatementQuietly(stDeleteDecisionRecord);
            DBUtil.closeStatementQuietly(stDeleteCoveragePremiumRecord);
            DBUtil.closeStatementQuietly(stDeleteCoverageExclusionRecord);
            DBUtil.closeStatementQuietly(stDeleteAdjournmentRecord);
            DBUtil.closeConnectionQuietly(conn);
        }
    }

    public void upsertNote(Note note, LocalDateTime now) throws ServiceException {
        try (Connection conn = this.risksDataSource.getConnection()) {
            conn.setAutoCommit(false);

            String appNumber = note.applicationNumber();
            String migrationAuthor = "macao.migration@partners.axa";
            String referenceAppNumber = "2021A024046";

            RiskInfo riskInfo = getRiskInfoForApplication(appNumber, conn);
            Timestamp refCreated = getReferenceCreationDate(referenceAppNumber, conn);

            if (isPreMigration(riskInfo.creationDate(), refCreated)) {
                handlePreMigrationLogic(conn, riskInfo.riskId(), note, now, migrationAuthor);
            } else {
                handlePostMigrationLogic(conn, riskInfo.riskId(), note, now, migrationAuthor);
            }

            DBUtil.commit(conn);
        } catch (SQLException e) {
            String msg = String.format("Cannot upsert DMS Risk Note for application '%s': %s", note.applicationNumber(), e.getMessage());
            logger.error(msg);
            throw new ServiceException(msg);
        }

    }

    private RiskInfo getRiskInfoForApplication(String appNumber, Connection conn) throws SQLException, ServiceException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT r.Id, r.CreationDate FROM Risks r WHERE r.FileNumber = ?")) {
            ps.setString(1, appNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new ServiceException("Risk not found for application: " + appNumber);
                }
                return new RiskInfo(rs.getString(1), rs.getTimestamp(2));
            }
        }
    }

    private Timestamp getReferenceCreationDate(String referenceAppNumber, Connection conn) throws SQLException, ServiceException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT r.CreationDate FROM Risks r WHERE r.FileNumber = ?")) {
            ps.setString(1, referenceAppNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new ServiceException("Reference application not found: " + referenceAppNumber);
                }
                return rs.getTimestamp(1);
            }
        }
    }

    private boolean isPreMigration(Timestamp targetCreated, Timestamp referenceCreated) {
        return !targetCreated.after(referenceCreated);
    }

    private void handlePreMigrationLogic(Connection conn, String riskId, Note note, LocalDateTime now, String migrationAuthor) throws SQLException {
        try (PreparedStatement stPreMigration = conn.prepareStatement("SELECT n.Id, n.Description, n.CreationDate, n.AuthorId, n.NoteType " + "FROM Notes n WHERE n.RiskId = ? AND n.AuthorId = ? ORDER BY n.CreationDate")) {

            stPreMigration.setString(1, riskId);
            stPreMigration.setString(2, migrationAuthor);

            try (ResultSet rs = stPreMigration.executeQuery()) {
                if (rs.next()) {
                    String noteId = rs.getString(1);
                    String existingText = rs.getString(2);

                    if (noteId != null && !note.note().equals(existingText)) {
                        try (PreparedStatement stUpdate = conn.prepareStatement(
                                "UPDATE Notes SET ModificationDate = ?, Description = ? WHERE Id = ?");
                             PreparedStatement stHistory = conn.prepareStatement(
                                     "INSERT INTO NoteHistories (Id, CreationDate, ModificationDate, Description, AuthorId, NoteType, NoteId) " +
                                             "VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                            updateNote(stUpdate, noteId, now, note.note());
                            insertNoteHistory(stHistory, noteId, now, rs.getTimestamp(3), note.note(), rs.getString(4), rs.getInt(5));
                        }
                    }
                } else {
                    try (PreparedStatement stInsert = conn.prepareStatement(
                            "INSERT INTO Notes (Id, CreationDate, ModificationDate, Description, AuthorId, NoteType, RiskId) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                        String noteId = UUID.randomUUID().toString();
                        insertNote(stInsert, noteId, note.creationDate(), now, note.note(), migrationAuthor, 0, riskId);
                    }
                }
            }
        }
    }

    private void handlePostMigrationLogic(Connection conn, String riskId, Note note, LocalDateTime now, String migrationAuthor) throws SQLException {
        try (PreparedStatement stPostMigration = conn.prepareStatement("SELECT n.Description FROM Notes n WHERE n.RiskId = ? ORDER BY n.CreationDate")) {

            stPostMigration.setString(1, riskId);

            boolean noteAlreadyExists = false;
            try (ResultSet rs = stPostMigration.executeQuery()) {
                while (rs.next()) {
                    String existingText = rs.getString(1);
                    if (note.note().equals(existingText)) {
                        noteAlreadyExists = true;
                        break;
                    }
                }
            }

            if (!noteAlreadyExists) {
                try (PreparedStatement stInsert = conn.prepareStatement(
                        "INSERT INTO Notes (Id, CreationDate, ModificationDate, Description, AuthorId, NoteType, RiskId) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                    String noteId = UUID.randomUUID().toString();
                    insertNote(stInsert, noteId, note.creationDate(), now, note.note(), migrationAuthor, 0, riskId);
                }
            }
        }
    }

    private void updateNote(PreparedStatement stUpdate, String noteId, LocalDateTime now, String newText) throws SQLException {
        stUpdate.setTimestamp(1, Timestamp.valueOf(now));
        stUpdate.setString(2, newText);
        stUpdate.setString(3, noteId);
        stUpdate.executeUpdate();
    }

    private void insertNote(PreparedStatement stInsert, String noteId, LocalDateTime created, LocalDateTime modified, String text, String authorId, int noteType, String riskId) throws SQLException {
        stInsert.setString(1, noteId);
        stInsert.setTimestamp(2, Timestamp.valueOf(created));
        stInsert.setTimestamp(3, Timestamp.valueOf(modified));
        stInsert.setString(4, text);
        stInsert.setString(5, authorId);
        stInsert.setInt(6, noteType);
        stInsert.setString(7, riskId);
        stInsert.executeUpdate();
    }

    private void insertNoteHistory(PreparedStatement stHistory, String noteId, LocalDateTime modified, Timestamp created, String text, String authorId, int noteType) throws SQLException {
        stHistory.setString(1, UUID.randomUUID().toString());
        stHistory.setTimestamp(2, created);
        stHistory.setTimestamp(3, Timestamp.valueOf(modified));
        stHistory.setString(4, text);
        stHistory.setString(5, authorId);
        stHistory.setInt(6, noteType);
        stHistory.setString(7, noteId);
        stHistory.executeUpdate();
    }

    private record RiskInfo(String riskId, Timestamp creationDate) {
    }

    public DMSDecision getDecisionData(String decisionId) throws ServiceException {
        Connection conn = null;
        PreparedStatement stGetDecisionData = null;
        PreparedStatement stGetExtraPremiums = null;
        PreparedStatement stGetExclusions = null;
        PreparedStatement stGetCoverage = null;
        PreparedStatement stGetOption = null;
        PreparedStatement stGetAdjournments = null;
        ResultSet rsGetDecisionData = null;
        ResultSet rsGetExtraPremiums = null;
        ResultSet rsGetExclusions = null;
        ResultSet rsGetCoverage = null;
        ResultSet rsGetOption = null;
        ResultSet rsGetAdjournments = null;

        try {
            conn = this.decisionsDataSource.getConnection();
            stGetDecisionData = conn.prepareStatement("""
                SELECT LoanId, CoverageId, OptionId, CreationDate, ModificationDate, 
                RiskType, DecisionType, DecisionStatus, HasExtrapremium, HasContractualExclusion, 
                HasPartialExclusion, AdjournmentId, HasAmountLimit, AmountLimit, HasWarrantyLimit, WarrantyLimit
                FROM Decisions
                WHERE ID = ?
            """);

            stGetExtraPremiums = conn.prepareStatement("""
                SELECT ID, CreationDate, ModificationDate, LinkType, PremiumType, Rate, Unit, Duration
                FROM CoveragePremiums
                WHERE DecisionId = ?
            """);

            stGetExclusions = conn.prepareStatement("""
                SELECT ID, CreationDate, ModificationDate, ExclusionType, Code, Description
                FROM CoverageExclusions
                WHERE DecisionId =  ?
            """);

            stGetCoverage = conn.prepareStatement("""
                SELECT LoanId, Name, Quota, InsuredAmount
                FROM Coverages
                WHERE ID = ?
            """);

            stGetOption = conn.prepareStatement("""
                SELECT LoanId, Name, Description
                FROM Options
                WHERE ID = ?
            """);

            stGetAdjournments = conn.prepareStatement("""
                SELECT CreationDate, ModificationDate, AdjournmentType, AdjournmentDetail
                FROM Adjournments
                WHERE ID = ?
            """);

            stGetDecisionData.setString(1, decisionId);
            rsGetDecisionData = stGetDecisionData.executeQuery();
            DMSDecision result = new DMSDecision();
            result.setDecisionId(decisionId);
            String adjournmentId;
            while (rsGetDecisionData.next()) {
                result.setLoanId(rsGetDecisionData.getString(1));
                result.setCoverageId(rsGetDecisionData.getString(2));
                result.setOptionId(rsGetDecisionData.getString(3));
                Timestamp ts = rsGetDecisionData.getTimestamp(4);
                if (ts != null) {
                    result.setCreationDate(ts.toLocalDateTime());
                }

                ts = rsGetDecisionData.getTimestamp(5);
                if (ts != null) {
                    result.setModificationDate(ts.toLocalDateTime());
                }

                result.setRiskType(rsGetDecisionData.getInt(6));
                result.setDecisionType(rsGetDecisionData.getInt(7));
                result.setDecisionStatus(rsGetDecisionData.getInt(8));
                result.setHasExtraPremium(rsGetDecisionData.getBoolean(9) ? 1 : 0);
                result.setHasContractualExclusion(rsGetDecisionData.getBoolean(10) ? 1 : 0);
                result.setHasPartialExclusion(rsGetDecisionData.getBoolean(11) ? 1 : 0);
                result.setHasAmountLimit(rsGetDecisionData.getBoolean(13) ? 1 : 0);
                result.setAmountLimit(rsGetDecisionData.getString(14));
                result.setHasWarrantyLimit(rsGetDecisionData.getBoolean(15) ? 1 : 0);
                result.setWarrantyLimit(rsGetDecisionData.getString(16));
                adjournmentId = rsGetDecisionData.getString(12);

                // Load extra premiums
                if (rsGetDecisionData.getBoolean(9)) {
                    stGetExtraPremiums.setString(1, result.getDecisionId());
                    rsGetExtraPremiums = stGetExtraPremiums.executeQuery();
                    if (rsGetExtraPremiums.next()) {
                        CoveragePremium premium = new CoveragePremium();
                        premium.setCoverPremiumId(rsGetExtraPremiums.getString(1));
                        ts = rsGetExtraPremiums.getTimestamp(2);
                        if (ts != null) {
                            premium.setCreationDate(ts.toLocalDateTime());
                        }
                        ts = rsGetExtraPremiums.getTimestamp(3);
                        if (ts != null) {
                            premium.setModificationDate(ts.toLocalDateTime());
                        }

                        premium.setLinkType(rsGetExtraPremiums.getInt(4));
                        premium.setPremiumType(rsGetExtraPremiums.getInt(5));
                        premium.setPremiumRate(rsGetExtraPremiums.getInt(6));
                        premium.setUnit(rsGetExtraPremiums.getInt(7));
                        premium.setDuration(rsGetExtraPremiums.getInt(8));
                        result.setCoveragePremium(premium);
                    }
                }

                // Load exclusions
                if (rsGetDecisionData.getBoolean(10) || rsGetDecisionData.getBoolean(11)) {
                    stGetExclusions.setString(1, result.getDecisionId());
                    rsGetExclusions = stGetExclusions.executeQuery();
                    while (rsGetExclusions.next()) {
                        Exclusion exclusion = new Exclusion();
                        exclusion.setCoverExclusionId(rsGetExclusions.getString(1));

                        ts = rsGetExclusions.getTimestamp(2);
                        if (ts != null) {
                            exclusion.setCreationDate(ts.toLocalDateTime());
                        }
                        ts = rsGetExclusions.getTimestamp(3);
                        if (ts != null) {
                            exclusion.setModificationDate(ts.toLocalDateTime());
                        }

                        exclusion.setType(rsGetExclusions.getInt(4));
                        exclusion.setCode(rsGetExclusions.getString(5));
                        exclusion.setDescription(rsGetExclusions.getString(6));

                        result.addExclusion(exclusion);
                    }
                }

                // Load Adjournment data
                if (adjournmentId != null) {
                    stGetAdjournments.setString(1, adjournmentId);
                    rsGetAdjournments = stGetAdjournments.executeQuery();
                    if (rsGetAdjournments.next()) {
                        Adjournment adjournment = new Adjournment();
                        adjournment.setId(adjournmentId);
                        ts = rsGetAdjournments.getTimestamp(1);
                        if (ts != null) {
                            adjournment.setCreationDate(ts.toLocalDateTime());
                        }
                        ts = rsGetAdjournments.getTimestamp(2);
                        if (ts != null) {
                            adjournment.setModificationDate(ts.toLocalDateTime());
                        }

                        adjournment.setType(rsGetAdjournments.getInt(3));
                        adjournment.setDetails(rsGetAdjournments.getString(4));
                        result.setAdjournment(adjournment);
                    }
                }

                // Load coverage data
                if (result.getCoverageId() != null) {
                    stGetCoverage.setString(1, result.getCoverageId());
                    rsGetCoverage = stGetCoverage.executeQuery();

                    if (rsGetCoverage.next()) {
                        result.setCoverage(true);
                        result.setLoanId(rsGetCoverage.getString(1));
                        result.setCoverageName(rsGetCoverage.getInt(2));
                        result.setCoverageQuota(rsGetCoverage.getInt(3));
                        result.setCoverageInsuredAmount(rsGetCoverage.getInt(4));
                    }
                } else if (result.getOptionId() != null) {
                    stGetOption.setString(1, result.getOptionId());
                    rsGetOption = stGetOption.executeQuery();
                    if (rsGetOption.next()) {
                        result.setCoverage(false);
                        result.setLoanId(rsGetOption.getString(1));
                        result.setCoverageName(rsGetOption.getInt(2));
                        result.setOptionDescription(rsGetOption.getString(3));
                    }
                }


            }

            return result;
        } catch (SQLException e) {
            logger.error("error", e);
            throw new ServiceException(String.format("getDecisionData - Error retrieving DMS decision: %s, message - %s",
                    decisionId, e.getMessage()));
        } finally {
            DBUtil.closeResultSetQuietly(rsGetAdjournments);
            DBUtil.closeResultSetQuietly(rsGetOption);
            DBUtil.closeResultSetQuietly(rsGetCoverage);
            DBUtil.closeResultSetQuietly(rsGetExclusions);
            DBUtil.closeResultSetQuietly(rsGetExtraPremiums);
            DBUtil.closeResultSetQuietly(rsGetDecisionData);
            DBUtil.closeStatementQuietly(stGetAdjournments);
            DBUtil.closeStatementQuietly(stGetOption);
            DBUtil.closeStatementQuietly(stGetCoverage);
            DBUtil.closeStatementQuietly(stGetExclusions);
            DBUtil.closeStatementQuietly(stGetExtraPremiums);
            DBUtil.closeStatementQuietly(stGetDecisionData);
            DBUtil.closeConnectionQuietly(conn);
        }
    }
}