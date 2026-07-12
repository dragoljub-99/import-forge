using ImportForge.Domain;
using ImportForge.Infrastructure.Db;
using ImportForge.Infrastructure.Repositories;

using Microsoft.Data.Sqlite;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportRowRepairService
{
    private const string UnknownFieldName = "unknown";

    private readonly ImportJobsRepository _jobsRepository;
    private readonly ImportRowsRepository _rowsRepository;
    private readonly ImportRowErrorsRepository _rowErrorsRepository;
    private readonly DbConnectionFactory _dbConnectionFactory;

    public ImportRowRepairService(
        ImportJobsRepository jobsRepository,
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository,
        DbConnectionFactory dbConnectionFactory)
    {
        _jobsRepository = jobsRepository;
        _rowsRepository = rowsRepository;
        _rowErrorsRepository = rowErrorsRepository;
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task<ImportRowRepairResult> RepairAsync(
        long jobId,
        int rowNumber,
        StagedImportRowPatch patch,
        CancellationToken ct)
    {
        if (!patch.HasAnyField)
        {
            return new ImportRowRepairResult(ImportRowRepairResultStatus.NoFieldsProvided);
        }
        await using var connection = await _dbConnectionFactory.OpenConnectionAsync(ct);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

        try
        {
            var job = await _jobsRepository.GetByIdAsync(connection, transaction, jobId, ct);

            if (job is null)
            {
                return new ImportRowRepairResult(ImportRowRepairResultStatus.JobNotFound);
            }

            if (job.Status != ImportJobStatus.NeedsFixes)
            {
                return new ImportRowRepairResult(ImportRowRepairResultStatus.JobNotInNeedsFixes);
            }

            var row = await _rowsRepository.GetByJobIdAndRowNumberAsync(connection, transaction, jobId,
                                                                        rowNumber, ct);
            if (row is null)
            {
                return new ImportRowRepairResult(ImportRowRepairResultStatus.RowNotFound);
            }

            var currentRowErrors = await _rowErrorsRepository.ListByRowIdAsync(connection, transaction,
                                                                               row.Id, ct);
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
                connection,
                transaction,
                row.Id,
                updatedRow.ProductId,
                updatedRow.ProductName,
                updatedRow.ProductRsdValue,
                updatedRow.ProductQuantity,
                ct);

            await _rowErrorsRepository.DeleteByRowIdAsync(connection, transaction, row.Id, ct);

            foreach (var error in StagedImportRowValidator.Validate(updatedRow))
            {
                await _rowErrorsRepository.AddAsync(connection, transaction, row.Id, error.Field, error.Error, ct);
            }

            var totalRows = await _rowsRepository.CountByJobIdAsync(connection, transaction, jobId, ct);
            var invalidRows = await _rowErrorsRepository.CountDistinctRowsWithErrorsByJobIdAsync(connection,
                                                                                                 transaction,
                                                                                                 jobId, ct);
            var validRows = totalRows - invalidRows;

            await _jobsRepository.UpdateCountersAsync(connection, transaction,
                                                      jobId, totalRows, validRows, invalidRows, ct);
            await _jobsRepository.UpdateStatusAsync(connection, transaction,
                                                    jobId, ImportJobStatus.NeedsFixes, ct);

            await transaction.CommitAsync(ct);
            return new ImportRowRepairResult(ImportRowRepairResultStatus.Repaired);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }

    }
}
