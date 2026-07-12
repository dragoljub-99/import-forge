using ImportForge.Infrastructure.Storage;
using ImportForge.Infrastructure.Repositories;
using ImportForge.Infrastructure.Db;
using Microsoft.Extensions.Logging;
using ImportForge.Domain;
using Microsoft.Data.Sqlite;

namespace ImportForge.Infrastructure.Processing
{
    public class ImportJobRecoveryService
    {
        private readonly ImportFileStorage _importFileStorage;
        private readonly ImportJobsRepository _importJobsRepository;
        private readonly ImportRowsRepository _importRowsRepository;
        private readonly ILogger<ImportJobRecoveryService> _logger;
        private readonly DbConnectionFactory _dbConnectionFactory;


        public ImportJobRecoveryService(ImportFileStorage importFileStorage,
                                        ImportJobsRepository importJobsRepository,
                                        ImportRowsRepository importRowsRepository,
                                        ILogger<ImportJobRecoveryService> logger,
                                        DbConnectionFactory dbConnectionFactory)
        {
            _importFileStorage = importFileStorage;
            _importJobsRepository = importJobsRepository;
            _importRowsRepository = importRowsRepository;
            _logger = logger;
            _dbConnectionFactory = dbConnectionFactory;
        }

        public async Task<ImportJobRecoveryResult> RecoverAsync(CancellationToken ct)
        {
            var jobIds = await _importJobsRepository.GetIdsByStatusAsync(
                ImportJobStatus.Processing,
                ct);

            if (jobIds.Count == 0)
            {
                _logger.LogDebug(
                    "No interrupted import jobs were found during startup recovery.");

                return new ImportJobRecoveryResult(
                    RecoveredJobs: 0,
                    DeletedRows: 0,
                    DeletedFiles: 0);
            }

            var recoveredJobsCount = 0;
            var deletedRowsCount = 0;
            var deletedFilesCount = 0;

            foreach (var jobId in jobIds)
            {
                ct.ThrowIfCancellationRequested();

                var deletedRows = await RecoverJobDataBaseStateAsync(jobId, ct);

                var fileDeleted = false;

                try
                {
                    fileDeleted = _importFileStorage.DeleteIfExists(jobId);
                }
                catch (IOException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to delete uploaded file for interrupted import job {JobId}.",
                        jobId);
                }

                recoveredJobsCount++;
                deletedRowsCount += deletedRows;

                if (fileDeleted)
                {
                    deletedFilesCount++;
                }

                _logger.LogInformation(
                    "Interrupted import job {JobId} was marked as failed. Deleted staging rows: {DeletedRows}. Uploaded file deleted: {FileDeleted}.",
                    jobId,
                    deletedRows,
                    fileDeleted);
            }

            _logger.LogInformation(
                "Startup import job recovery completed. Recovered jobs: {RecoveredJobs}, deleted staging rows: {DeletedRows}, deleted uploaded files: {DeletedFiles}.",
                recoveredJobsCount,
                deletedRowsCount,
                deletedFilesCount);

            return new ImportJobRecoveryResult(
                RecoveredJobs: recoveredJobsCount,
                DeletedRows: deletedRowsCount,
                DeletedFiles: deletedFilesCount);
        }

        private async Task<int> RecoverJobDataBaseStateAsync(long jobId, CancellationToken ct)
        {
            await using var connection = await _dbConnectionFactory.OpenConnectionAsync();
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

            try
            {
                await _importJobsRepository.UpdateStatusAsync(connection, transaction, jobId,
                                                              ImportJobStatus.Failed, ct);

                var deletedRows = await _importRowsRepository.DeleteByJobIdAsync(
                                                             connection,
                                                             transaction,
                                                             jobId, ct);

                await transaction.CommitAsync(ct);

                return deletedRows;
            }
            catch
            {
                await transaction.RollbackAsync(CancellationToken.None);
                throw;
            }
        }
    }
}