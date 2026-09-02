package org.upyog.dashboard.service;

import org.upyog.dashboard.constants.DashboardExtractorConstants;
import org.upyog.dashboard.repository.IngestionSummaryRepository;
import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.upyog.dashboard.api.DashboardIngestionClient;
import org.upyog.dashboard.config.DashboardProperties;
import org.upyog.dashboard.common.constants.Module;
import org.upyog.dashboard.extractor.LegacyBatchExtractor;
import org.upyog.dashboard.model.IngestionResult;
import org.upyog.dashboard.model.LegacyIngestionResponse;

import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.upyog.dashboard.registry.ExtractorRegistry;
import org.upyog.dashboard.extractor.ModuleExtractor;
import org.upyog.dashboard.entity.DailyIngestionData;
import org.upyog.dashboard.enums.IngestionStatus;
import org.upyog.dashboard.util.CommonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Orchestrator handling the heavy-duty manual legacy batch ingestion processes.
 * <p>
 * This service manages memory-safe streaming of large historical datasets, dynamically routing them
 * either directly to the internal API or uploading physical chunked files to the egov-filestore based on configuration.
 * It strictly enforces concurrency via ShedLock to prevent memory exhaustion from overlapping batch requests.
 * </p>
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class LegacyBatchIngestionOrchestrator {

    private final LegacyBatchExtractor batchExtractor;
    private final SXSSFExcelGeneratorService excelGeneratorService;
    private final DashboardIngestionClient ingestionClient;
    private final DashboardProperties dashboardProperties;
    private final LockProvider lockProvider;
    private final IngestionPersistenceService persistenceService;
    private final IngestionSummaryRepository summaryRepository;
    private final ExtractorRegistry extractorRegistry;
    private final ObjectMapper objectMapper;

    @Value("${dashboard-data.legacy.batch-size:500}")
    private int batchSize;

    @Value("${dashboard-data.legacy.keep-excel-file:true}")
    private boolean keepExcelFile;

    /**
     * Executes legacy extraction into a single Excel file by streaming DB batches incrementally,
     * then posts the generated Excel file to the legacy API endpoint.
     *
     * @param request legacy batch request containing startDate, endDate, and moduleName
     * @return LegacyIngestionResponse summarizing execution outcome
     */
    public LegacyIngestionResponse processLegacyBatchIngest(LegacyBatchIngestRequest request) {
        String tenantId = dashboardProperties.getTenantId();
        LocalDate start = LocalDate.parse(request.getStartDate());
        LocalDate end = LocalDate.parse(request.getEndDate());
        String moduleName = request.getModuleName();

        if (start.isAfter(end)) {
            String errorMsg = "Invalid date range: startDate (" + start + ") cannot be after endDate (" + end + ")";
            log.warn(errorMsg);
            return LegacyIngestionResponse.builder()
                    .totalDatesRequested(0)
                    .datesFailed(1)
                    .processedResults(List.of(IngestionResult.builder()
                            .ingestionStatus(DashboardExtractorConstants.STATUS_FAILURE)
                            .failureReason(errorMsg)
                            .build()))
                    .build();
        }

        // Check for already successfully ingested overlapping legacy records
        List<IngestionSummaryRepository.LegacyJob> overlappingJobs = summaryRepository
                .findOverlappingSuccessfulLegacyJobs(tenantId, moduleName, start, end);

        if (!overlappingJobs.isEmpty()) {
            String overlapMsg = String.format("Request aborted: Legacy data for module '%s' and date range [%s to %s] overlaps with %d already successfully ingested record(s).",
                    moduleName, start, end, overlappingJobs.size());
            log.warn(overlapMsg);
            return LegacyIngestionResponse.builder()
                    .totalDatesRequested((int) start.until(end.plusDays(1)).getDays())
                    .datesSkipped(overlappingJobs.size())
                    .datesProcessedSuccessfully(0)
                    .datesFailed(1)
                    .processedResults(List.of(IngestionResult.builder()
                            .ingestionStatus(DashboardExtractorConstants.STATUS_FAILURE)
                            .failureReason(overlapMsg)
                            .build()))
                    .build();
        }

        String jobId = "JOB-" + moduleName.toUpperCase() + "-" + UUID.randomUUID().toString().substring(0, 8);

        log.info("Processing legacy batch ingestion job {} for module {} (date range: {} to {}, tenantId: {}, batchChunkSize: {})",
                jobId, moduleName, start, end, tenantId, batchSize);

        String lockName = "manual_batch_extraction_" + moduleName.toUpperCase();
        LockConfiguration lockConfig = new LockConfiguration(
                Instant.now(), 
                lockName, 
                Duration.ofHours(3), 
                Duration.ofMinutes(1)
        );

        Optional<SimpleLock> lock = lockProvider.lock(lockConfig);
        if (lock.isEmpty()) {
            log.warn("Job {} aborted: A batch extraction job is already running for module {}", jobId, moduleName);
            return LegacyIngestionResponse.builder()
                    .totalDatesRequested(0)
                    .datesFailed(1)
                    .processedResults(List.of(IngestionResult.builder()
                            .ingestionStatus(DashboardExtractorConstants.STATUS_FAILURE)
                            .failureReason("A batch extraction job is currently in progress for module " + moduleName + ". Please wait for it to complete.")
                            .build()))
                    .build();
        }

        // Register initial legacy job audit entry with full range and execution date
        persistenceService.createLegacyJob(jobId, tenantId, moduleName, LocalDate.now(), start, end);

        File generatedExcelFile = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DashboardExtractorConstants.DATE_FORMAT);

        // Map tracking per-date candidate status and sample request data
        Map<LocalDate, DateCandidateAudit> dateAuditMap = new LinkedHashMap<>();
        LocalDate cur = start;
        while (!cur.isAfter(end)) {
            dateAuditMap.put(cur, new DateCandidateAudit(IngestionStatus.MISSED_DATE.getValue(), null, tenantId));
            cur = cur.plusDays(1);
        }

        try (SXSSFExcelGeneratorService.StreamingExcelSession session = excelGeneratorService.createStreamingSession(moduleName)) {

            Module module = Module.valueOf(moduleName.toUpperCase());
            ModuleExtractor<?> extractor = extractorRegistry != null ? extractorRegistry.get(module) : null;

            // Step 1: Extractor queries DB date-by-date and streams rows directly to single Excel session
            long totalExtracted = batchExtractor.extractInBatches(module, start, end, tenantId, batchSize, batchRecords -> {
                List<Object> nonZeroRecords = new ArrayList<>();

                // Analyze records to determine candidate date status and filter non-zero records for Excel
                for (Object rec : batchRecords) {
                    LocalDate recordDate = extractDateFromRecord(rec);
                    boolean isZero = isRecordZeroMetrics(extractor, rec);

                    if (!isZero) {
                        nonZeroRecords.add(rec);
                    }

                    if (recordDate != null && dateAuditMap.containsKey(recordDate)) {
                        DateCandidateAudit candidate = dateAuditMap.get(recordDate);
                        String candidateStatus = isZero ? IngestionStatus.SUCCESS_ZERO_METRICS.getValue() : IngestionStatus.SUCCESS.getValue();
                        
                        // If previously MISSED_DATE or SUCCESS_ZERO_METRICS, upgrade if non-zero data found
                        if (IngestionStatus.MISSED_DATE.getValue().equals(candidate.status) ||
                                (IngestionStatus.SUCCESS_ZERO_METRICS.getValue().equals(candidate.status) && !isZero)) {
                            candidate.status = candidateStatus;
                        }

                        if (candidate.samplePayloadJson == null) {
                            try {
                                candidate.samplePayloadJson = objectMapper != null ? objectMapper.writeValueAsString(rec) : rec.toString();
                            } catch (Exception e) {
                                candidate.samplePayloadJson = rec.toString();
                            }
                        }

                        String recTenant = extractTenantId(rec, tenantId);
                        if (recTenant != null) {
                            candidate.tenantId = recTenant;
                        }
                    }
                }

                if (!nonZeroRecords.isEmpty()) {
                    log.info("Streaming DB batch chunk of {} non-zero records to Excel file session (skipped {} zero-metric records)...",
                            nonZeroRecords.size(), batchRecords.size() - nonZeroRecords.size());
                    session.appendBatchRecords(nonZeroRecords);
                } else {
                    log.info("Skipped adding {} zero-metric records to Excel file session.", batchRecords.size());
                }
            });

            if (totalExtracted == 0) {
                log.info("No records found for legacy extraction job {}. Skipping Excel generation.", jobId);
                String emptyResponse = "{\"message\": \"No records found for specified date range\"}";
                persistenceService.updateLegacyJobStatus(jobId, DashboardExtractorConstants.STATUS_SUCCESS, null, emptyResponse);

                // Persist MISSED_DATE audit entries in ingestion_detail
                persistDateWiseAudits(dateAuditMap, moduleName, emptyResponse, false);

                return LegacyIngestionResponse.builder()
                        .totalDatesRequested((int) start.until(end.plusDays(1)).getDays())
                        .datesSkipped(0)
                        .datesProcessedSuccessfully(0)
                        .datesFailed(0)
                        .skippedDates(List.of())
                        .processedResults(List.of(IngestionResult.builder()
                                .ingestionStatus(DashboardExtractorConstants.STATUS_SUCCESS)
                                .failureReason("No records found for specified date range")
                                .build()))
                        .build();
            }

            // Step 2: Finalize single combined Excel file
            generatedExcelFile = session.finishWorkbook();

            // Step 3: Send generated Excel file to appropriate endpoint via unified ingestion client
            String legacyMode = dashboardProperties.getEffectiveLegacyUploadMode();
            IngestionResult ingestionResult = ingestionClient.ingest(generatedExcelFile, moduleName, tenantId, legacyMode);

            boolean isSuccess = DashboardExtractorConstants.STATUS_SUCCESS.equalsIgnoreCase(ingestionResult.getIngestionStatus());
            String responseJson = ingestionResult.getResponseData() != null 
                    ? ingestionResult.getResponseData() 
                    : "{\"failureReason\": \"" + (ingestionResult.getFailureReason() != null ? ingestionResult.getFailureReason().replace("\"", "'") : "Unknown Error") + "\"}";

            // Persist the status and fileStoreId into legacy_data_ingestion_detail
            persistenceService.updateLegacyJobStatus(jobId, ingestionResult.getIngestionStatus(), null, responseJson);

            // Persist per-date audit entries into ingestion_detail
            persistDateWiseAudits(dateAuditMap, moduleName, responseJson, isSuccess);

            return LegacyIngestionResponse.builder()
                    .totalDatesRequested((int) start.until(end.plusDays(1)).getDays())
                    .datesSkipped(0)
                    .datesProcessedSuccessfully(isSuccess ? 1 : 0)
                    .datesFailed(isSuccess ? 0 : 1)
                    .skippedDates(List.of())
                    .processedResults(List.of(ingestionResult))
                    .build();

        } catch (Exception exception) {
            log.error("Error executing streaming legacy batch ingestion job {}: {}", jobId, exception.getMessage(), exception);
            String errResponse = "{\"error\": \"" + (exception.getMessage() != null ? exception.getMessage().replace("\"", "'") : "Exception") + "\"}";
            persistenceService.updateLegacyJobStatus(jobId, DashboardExtractorConstants.STATUS_FAILURE, null, errResponse);

            // Persist per-date failure audit entries into ingestion_detail
            persistDateWiseAudits(dateAuditMap, moduleName, errResponse, false);

            return LegacyIngestionResponse.builder()
                    .totalDatesRequested((int) start.until(end.plusDays(1)).getDays())
                    .datesFailed(1)
                    .processedResults(List.of(IngestionResult.builder()
                            .ingestionStatus(DashboardExtractorConstants.STATUS_FAILURE)
                            .failureReason(exception.getMessage())
                            .build()))
                    .build();
        } finally {
            if (generatedExcelFile != null && generatedExcelFile.exists()) {
                if (keepExcelFile) {
                    log.info("PRESERVED single legacy Excel file at: {}", generatedExcelFile.getAbsolutePath());
                } else {
                    boolean deleted = generatedExcelFile.delete();
                    log.info("Temporary Excel file deletion status for job {}: {}", jobId, deleted);
                }
            }
            lock.get().unlock();
            log.info("Released ShedLock for module {}", moduleName);
        }
    }

    private void persistDateWiseAudits(Map<LocalDate, DateCandidateAudit> dateAuditMap, String moduleName, String responseData, boolean overallSuccess) {
        if (dateAuditMap == null || dateAuditMap.isEmpty()) {
            return;
        }
        List<DailyIngestionData> auditRecords = new ArrayList<>();
        long now = CommonUtils.getCurrentEpochMillis();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DashboardExtractorConstants.DATE_FORMAT);

        for (Map.Entry<LocalDate, DateCandidateAudit> entry : dateAuditMap.entrySet()) {
            LocalDate date = entry.getKey();
            DateCandidateAudit candidate = entry.getValue();

            String finalStatus;
            if (!overallSuccess && !IngestionStatus.MISSED_DATE.getValue().equals(candidate.status)) {
                finalStatus = IngestionStatus.FAILURE.getValue();
            } else {
                finalStatus = candidate.status;
            }

            DailyIngestionData audit = DailyIngestionData.builder()
                    .moduleIngestionId(CommonUtils.generateUUID())
                    .tenantId(candidate.tenantId)
                    .moduleName(moduleName)
                    .pushDate(date.format(formatter))
                    .requestData(candidate.samplePayloadJson)
                    .responseData(responseData)
                    .ingestionStatus(finalStatus)
                    .createdBy("SYSTEM")
                    .createdTime(now)
                    .lastModifiedBy("SYSTEM")
                    .lastModifiedTime(now)
                    .build();

            auditRecords.add(audit);
        }

        log.info("Saving {} per-date ingestion_detail audit records for legacy batch {}", auditRecords.size(), moduleName);
        persistenceService.saveIngestionDetailsBatch(auditRecords);
    }

    private LocalDate extractDateFromRecord(Object item) {
        if (item == null) return null;
        try {
            java.lang.reflect.Method getDateMethod = item.getClass().getMethod("getDate");
            Object val = getDateMethod.invoke(item);
            if (val != null) {
                if (val instanceof LocalDate ld) return ld;
                if (val instanceof String s && !s.isBlank()) {
                    String str = s.trim();
                    try {
                        return LocalDate.parse(str);
                    } catch (Exception e) {
                        return LocalDate.parse(str, DateTimeFormatter.ofPattern(DashboardExtractorConstants.DATE_FORMAT));
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private String extractTenantId(Object item, String fallback) {
        if (item == null) return fallback;
        try {
            java.lang.reflect.Method getUlb = item.getClass().getMethod("getUlb");
            Object val = getUlb.invoke(item);
            if (val != null && !val.toString().isBlank()) return val.toString();
        } catch (Exception ignored) {
        }
        try {
            java.lang.reflect.Method getTenantId = item.getClass().getMethod("getTenantId");
            Object val = getTenantId.invoke(item);
            if (val != null && !val.toString().isBlank()) return val.toString();
        } catch (Exception ignored) {
        }
        return fallback;
    }

    private boolean isRecordZeroMetrics(ModuleExtractor<?> extractor, Object item) {
        if (item == null) return true;
        if (extractor != null) {
            return extractor.isZeroMetrics(item);
        }
        return false;
    }

    private static class DateCandidateAudit {
        String status;
        String samplePayloadJson;
        String tenantId;

        DateCandidateAudit(String status, String samplePayloadJson, String tenantId) {
            this.status = status;
            this.samplePayloadJson = samplePayloadJson;
            this.tenantId = tenantId;
        }
    }
}

