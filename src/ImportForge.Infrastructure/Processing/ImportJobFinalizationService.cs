using ImportForge.Domain;
using ImportForge.Infrastructure.Repositories;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobFinalizationService
{
    private readonly ImportJobsRepository _jobsRepository;
    private readonly ImportJobStagingRevalidationService _revalidationService;
    private readonly ImportJobAutoCommitService _autoCommitService;

    public ImportJobFinalizationService(
        ImportJobsRepository jobsRepository,
        ImportJobStagingRevalidationService revalidationService,
        ImportJobAutoCommitService autoCommitService)
    {
        _jobsRepository = jobsRepository;
        _revalidationService = revalidationService;
        _autoCommitService = autoCommitService;
    }

    public async Task<ImportJobFinalizationResult> FinalizeFromStagingAsync(long jobId, CancellationToken ct)
    {
        var job = await _jobsRepository.GetByIdAsync(jobId, ct);
        if (job is null)
        {
            return new ImportJobFinalizationResult(ImportJobFinalizationResultStatus.JobNotFound);
        }

        if (job.Status != ImportJobStatus.NeedsFixes)
        {
            return new ImportJobFinalizationResult(ImportJobFinalizationResultStatus.JobNotInNeedsFixes);
        }

        var validationSnapshot = await _revalidationService.RevalidateAndRefreshCountersAsync(jobId, ct);

        if (validationSnapshot.InvalidRows > 0)
        {
            await _jobsRepository.UpdateStatusAsync(jobId, ImportJobStatus.NeedsFixes, ct);

            return new ImportJobFinalizationResult(
                ImportJobFinalizationResultStatus.InvalidRowsRemain,
                validationSnapshot.TotalRows,
                validationSnapshot.ValidRows,
                validationSnapshot.InvalidRows);
        }

        var commitOutcome = await _autoCommitService.ExecuteAsync(jobId, validationSnapshot.TotalRows, ct);
        if (commitOutcome == ImportJobAutoCommitOutcome.Committed)
        {
            return new ImportJobFinalizationResult(
                ImportJobFinalizationResultStatus.Committed,
                validationSnapshot.TotalRows,
                validationSnapshot.ValidRows,
                validationSnapshot.InvalidRows);
        }

        var updatedJob = await _jobsRepository.GetByIdAsync(jobId, ct);

        return new ImportJobFinalizationResult(
            ImportJobFinalizationResultStatus.CommitConflictsDetected,
            updatedJob?.TotalRows ?? validationSnapshot.TotalRows,
            updatedJob?.ValidRows ?? validationSnapshot.ValidRows,
            updatedJob?.InvalidRows ?? validationSnapshot.InvalidRows);
    }
}

public enum ImportJobFinalizationResultStatus
{
    JobNotFound = 0,
    JobNotInNeedsFixes = 1,
    InvalidRowsRemain = 2,
    Committed = 3,
    CommitConflictsDetected = 4
}

public sealed record ImportJobFinalizationResult(
    ImportJobFinalizationResultStatus Status,
    int TotalRows = 0,
    int ValidRows = 0,
    int InvalidRows = 0);
