package org.upyog.dashboard.repository.querybuilder;

/**
 * Query constants class for SQL queries related to ingestion summary persistence.
 * Defines immutable, reusable SQL query strings.
 */
public final class IngestionSummaryQueryBuilder {

    private IngestionSummaryQueryBuilder() {
        // Prevent instantiation
    }

    public static final String SELECT_LAST_SUCCESSFUL_DATE_QUERY =
            "SELECT last_successful_date FROM ingestion_module_summary WHERE tenant_id = :tenantId AND module_name = :moduleName";

    public static final String UPSERT_LAST_SUCCESSFUL_DATE_QUERY =
            "INSERT INTO ingestion_module_summary (" +
            "   id, tenant_id, module_name, last_successful_date, last_attempted_date, created_by, created_time, last_modified_by, last_modified_time" +
            ") VALUES (:id, :tenantId, :moduleName, :lastSuccessfulDate, :lastAttemptedDate, :createdBy, :createdTime, :lastModifiedBy, :lastModifiedTime) " +
            "ON CONFLICT (tenant_id, module_name) " +
            "DO UPDATE SET last_successful_date = EXCLUDED.last_successful_date, " +
            "              last_attempted_date = EXCLUDED.last_attempted_date, " +
            "              last_modified_by = EXCLUDED.last_modified_by, " +
            "              last_modified_time = EXCLUDED.last_modified_time";

    public static final String UPSERT_LAST_ATTEMPTED_DATE_QUERY =
            "INSERT INTO ingestion_module_summary (" +
            "   id, tenant_id, module_name, last_successful_date, last_attempted_date, created_by, created_time, last_modified_by, last_modified_time" +
            ") VALUES (:id, :tenantId, :moduleName, :lastSuccessfulDate, :lastAttemptedDate, :createdBy, :createdTime, :lastModifiedBy, :lastModifiedTime) " +
            "ON CONFLICT (tenant_id, module_name) " +
            "DO UPDATE SET last_attempted_date = EXCLUDED.last_attempted_date, " +
            "              last_modified_by = EXCLUDED.last_modified_by, " +
            "              last_modified_time = EXCLUDED.last_modified_time";

    public static final String SELECT_SUCCESSFUL_DATES_IN_RANGE_QUERY =
            "SELECT DISTINCT push_date FROM (" +
            "  SELECT push_date FROM ingestion_detail WHERE tenant_id = :tenantId AND module_name = :moduleName AND ingestion_status = 'SUCCESS' AND push_date >= :startDate AND push_date <= :endDate " +
            "  UNION " +
            "  SELECT push_date FROM legacy_data_ingestion_detail WHERE tenant_id = :tenantId AND module_name = :moduleName AND ingestion_status = 'SUCCESS' AND push_date >= :startDate AND push_date <= :endDate " +
            ") combined_dates";

    public static final String UPDATE_MODULE_DETAIL_TABLE_QUERY =
            "UPDATE ingestion_module_detail SET last_ingested_date = :lastIngestedDate, is_legacy_data_ingested = TRUE, last_modified_time = :lastModifiedTime " +
            "WHERE tenant_id = :tenantId AND module_name = :moduleName";

    public static final String SELECT_LEGACY_JOB_DATES_QUERY =
            "SELECT DISTINCT push_date FROM legacy_data_ingestion_detail WHERE tenant_id = :tenantId AND module_name = :moduleName";

    public static final String INSERT_LEGACY_JOB_QUERY =
            "INSERT INTO legacy_data_ingestion_detail (" +
            "   module_ingestion_id, tenant_id, module_name, push_date, start_date, end_date, ingestion_status, exception_code, created_by, created_time, last_modified_by, last_modified_time" +
            ") VALUES (:id, :tenantId, :moduleName, :pushDate, :startDate, :endDate, :status, :exceptionCode, :createdBy, :createdTime, :lastModifiedBy, :lastModifiedTime)";

    public static final String SELECT_PENDING_OR_FAILED_LEGACY_JOBS_QUERY =
            "SELECT module_ingestion_id, push_date FROM legacy_data_ingestion_detail " +
            "WHERE tenant_id = :tenantId AND module_name = :moduleName AND ingestion_status IN ('NOT_STARTED', 'FAILURE') " +
            "ORDER BY push_date ASC LIMIT :limit";

    public static final String UPDATE_LEGACY_JOB_STATUS_QUERY =
            "UPDATE legacy_data_ingestion_detail " +
            "SET ingestion_status = :status, request_data = :requestData::jsonb, response_data = :responseData::jsonb, last_modified_time = :lastModifiedTime " +
            "WHERE module_ingestion_id = :id";

    public static final String SELECT_FOR_UPDATE_SUMMARY_QUERY =
            "SELECT id FROM ingestion_module_summary WHERE tenant_id = :tenantId AND module_name = :moduleName FOR UPDATE";

    public static final String SELECT_OVERLAPPING_SUCCESSFUL_LEGACY_JOBS_QUERY =
            "SELECT module_ingestion_id, push_date, start_date, end_date " +
            "FROM legacy_data_ingestion_detail " +
            "WHERE tenant_id = :tenantId AND module_name = :moduleName AND ingestion_status = 'SUCCESS' " +
            "  AND ((start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= :endDate AND end_date >= :startDate) " +
            "       OR (push_date >= :startDate AND push_date <= :endDate))";

    public static final String INSERT_INGESTION_DETAIL_QUERY =
            "INSERT INTO ingestion_detail (" +
            "   module_ingestion_id, module_detail_id, tenant_id, module_name, push_date, request_data, response_data, ingestion_status, exception_code, created_by, created_time, last_modified_by, last_modified_time" +
            ") VALUES (:moduleIngestionId, :moduleDetailId, :tenantId, :moduleName, :pushDate, :requestData::jsonb, :responseData::jsonb, :ingestionStatus, :exceptionCode, :createdBy, :createdTime, :lastModifiedBy, :lastModifiedTime)";
}



