using ImportForge.Infrastructure.Repositories;
using ImportForge.Infrastructure.Db;

using Microsoft.Data.Sqlite;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobStagingRevalidationService
{
    private readonly ImportJobsRepository _jobsRepository;
    private readonly ImportRowsRepository _rowsRepository;
    private readonly ImportRowErrorsRepository _rowErrorsRepository;
    private readonly DbConnectionFactory _dbConnectionFactory;

    public ImportJobStagingRevalidationService(
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

    public async Task<ImportJobValidationSnapshot> RevalidateAndRefreshCountersAsync(long jobId, CancellationToken ct)
    {
        await using var connection = await _dbConnectionFactory.OpenConnectionAsync(ct);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

        try
        {
            await _rowErrorsRepository.DeleteFieldLevelByJobIdAsync(connection, transaction, jobId, ct);

            var stagedRows = await _rowsRepository.ListValidatableByJobIdAsync(connection, transaction, 
                                                                               jobId, ct);
            foreach (var stagedRow in stagedRows)
            {
                foreach (var validationError in StagedImportRowValidator.Validate(stagedRow))
                {
                    await _rowErrorsRepository.AddAsync(connection, transaction, 
                                                        stagedRow.Id, validationError.Field, validationError.Error, ct);
                }
            }

            var totalRows = await _rowsRepository.CountByJobIdAsync(connection, transaction, jobId, ct);

            var invalidRows = await _rowErrorsRepository.CountDistinctRowsWithErrorsByJobIdAsync(
                                                                 connection,
                                                                 transaction,
                                                                 jobId, ct);
            var validRows = totalRows - invalidRows;

            await _jobsRepository.UpdateCountersAsync(connection, transaction,
                                                      jobId, totalRows, validRows, invalidRows, ct);
            
        
            await transaction.CommitAsync(ct);
            return new ImportJobValidationSnapshot(totalRows, validRows, invalidRows);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }
}

public sealed record ImportJobValidationSnapshot(
    int TotalRows,
    int ValidRows,
    int InvalidRows);
