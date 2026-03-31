using ImportForge.Domain;
using ImportForge.Infrastructure.Repositories;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportRowRepairService
{
    private const string UnknownFieldName = "unknown";

    private readonly ImportJobsRepository _jobsRepository;
    private readonly ImportRowsRepository _rowsRepository;
    private readonly ImportRowErrorsRepository _rowErrorsRepository;

    public ImportRowRepairService(
        ImportJobsRepository jobsRepository,
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository)
    {
        _jobsRepository = jobsRepository;
        _rowsRepository = rowsRepository;
        _rowErrorsRepository = rowErrorsRepository;
    }

    public async Task<ImportRowRepairResult> RepairAsync(
        long jobId,
        int rowNumber,
        StagedImportRowPatch patch,
        CancellationToken ct)
    {
        var job = await _jobsRepository.GetByIdAsync(jobId, ct);
        if (job is null)
        {
            return new ImportRowRepairResult(ImportRowRepairResultStatus.JobNotFound);
        }

        if (job.Status != ImportJobStatus.NeedsFixes)
        {
            return new ImportRowRepairResult(ImportRowRepairResultStatus.JobNotInNeedsFixes);
        }

        var row = await _rowsRepository.GetByJobIdAndRowNumberAsync(jobId, rowNumber, ct);
        if (row is null)
        {
            return new ImportRowRepairResult(ImportRowRepairResultStatus.RowNotFound);
        }

        if (!patch.HasAnyField)
        {
            return new ImportRowRepairResult(ImportRowRepairResultStatus.NoFieldsProvided);
        }

        var currentRowErrors = await _rowErrorsRepository.ListByRowIdAsync(row.Id, ct);
        var hasStructuralError = currentRowErrors.Any(error => string.Equals(error.Field, UnknownFieldName, StringComparison.Ordinal));

        if (hasStructuralError && !patch.HasAllBusinessFields)
        {
            return new ImportRowRepairResult(ImportRowRepairResultStatus.StructuralRowRequiresFullPayload);
        }

        var updatedRow = row with
        {
            ProductId = patch.HasProductId ? patch.ProductId : row.ProductId,
            ProductName = patch.HasProductName ? patch.ProductName : row.ProductName,
            ProductRsdValue = patch.HasProductRsdValue ? patch.ProductRsdValue : row.ProductRsdValue,
            ProductQuantity = patch.HasProductQuantity ? patch.ProductQuantity : row.ProductQuantity
        };

        await _rowsRepository.UpdateBusinessFieldsAsync(
            row.Id,
            updatedRow.ProductId,
            updatedRow.ProductName,
            updatedRow.ProductRsdValue,
            updatedRow.ProductQuantity,
            ct);

        await _rowErrorsRepository.DeleteByRowIdAsync(row.Id, ct);

        foreach (var error in StagedImportRowValidator.Validate(updatedRow))
        {
            await _rowErrorsRepository.AddAsync(row.Id, error.Field, error.Error, ct);
        }

        var totalRows = await _rowsRepository.CountByJobIdAsync(jobId, ct);
        var invalidRows = await _rowErrorsRepository.CountDistinctRowsWithErrorsByJobIdAsync(jobId, ct);
        var validRows = totalRows - invalidRows;

        await _jobsRepository.UpdateCountersAsync(jobId, totalRows, validRows, invalidRows, ct);
        await _jobsRepository.UpdateStatusAsync(jobId, ImportJobStatus.NeedsFixes, ct);

        return new ImportRowRepairResult(ImportRowRepairResultStatus.Repaired);
    }
}
