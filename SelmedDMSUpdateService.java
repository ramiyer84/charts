boolean updateSelmedDMSUpdatePackages(Connection conn, Entity updateEntity) throws ServiceException {
    // Inject special logic for Notes updates
    if (updateEntity instanceof SelmedDMSNotesUpdatePackage notesUpdatePackage) {
        logger.debug("Intercepting Notes update – invoking upsertNote");

        // Construct a Note domain object from the update package
        Note note = new Note(
            notesUpdatePackage.getNoteId(),
            notesUpdatePackage.getFileNumber(),
            notesUpdatePackage.getDescription(),
            notesUpdatePackage.getCreationDate()
        );

        // Use modificationDate as the 'now' timestamp for upsert
        this.upsertNote(note, notesUpdatePackage.getModificationDate());

        // Treat upsert as successful 'MODIFY' operation
        return true;
    }

    // Generic update logic for all other types (unchanged)
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