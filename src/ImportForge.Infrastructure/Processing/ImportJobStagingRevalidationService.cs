using ImportForge.Infrastructure.Repositories;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobStagingRevalidationService
{
    private readonly ImportJobsRepository _jobsRepository;
    private readonly ImportRowsRepository _rowsRepository;
    private readonly ImportRowErrorsRepository _rowErrorsRepository;

    public ImportJobStagingRevalidationService(
        ImportJobsRepository jobsRepository,
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository)
    {
        _jobsRepository = jobsRepository;
        _rowsRepository = rowsRepository;
        _rowErrorsRepository = rowErrorsRepository;
    }

    public async Task<ImportJobValidationSnapshot> RevalidateAndRefreshCountersAsync(long jobId, CancellationToken ct)
    {
        await _rowErrorsRepository.DeleteFieldLevelByJobIdAsync(jobId, ct);

        var stagedRows = await _rowsRepository.ListValidatableByJobIdAsync(jobId, ct);
        foreach (var stagedRow in stagedRows)
        {
            foreach (var validationError in StagedImportRowValidator.Validate(stagedRow))
            {
                await _rowErrorsRepository.AddAsync(stagedRow.Id, validationError.Field, validationError.Error, ct);
            }
        }

        var totalRows = await _rowsRepository.CountByJobIdAsync(jobId, ct);
        var invalidRows = await _rowErrorsRepository.CountDistinctRowsWithErrorsByJobIdAsync(jobId, ct);
        var validRows = totalRows - invalidRows;

        await _jobsRepository.UpdateCountersAsync(jobId, totalRows, validRows, invalidRows, ct);

        return new ImportJobValidationSnapshot(totalRows, validRows, invalidRows);
    }
}

public sealed record ImportJobValidationSnapshot(
    int TotalRows,
    int ValidRows,
    int InvalidRows);
