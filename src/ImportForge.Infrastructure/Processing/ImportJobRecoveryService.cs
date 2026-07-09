using ImportForge.Infrastructure.Storage;
using ImportForge.Infrastructure.Repositories;
using Microsoft.Extensions.Logging;
using ImportForge.Domain;

namespace ImportForge.Infrastructure.Processing
{
    public class ImportJobRecoveryService
    {
        private readonly ImportFileStorage _importFileStorage;
        private readonly ImportJobsRepository _importJobsRepository;
        private readonly ImportRowsRepository _importRowsRepository;
        private readonly ILogger<ImportJobRecoveryService> _logger;

        public ImportJobRecoveryService(ImportFileStorage importFileStorage,
                                        ImportJobsRepository importJobsRepository,
                                        ImportRowsRepository importRowsRepository,
                                        ILogger<ImportJobRecoveryService> logger)
        {
            _importFileStorage = importFileStorage;
            _importJobsRepository = importJobsRepository;
            _importRowsRepository = importRowsRepository;
            _logger = logger;
        }

        public async Task<ImportJobRecoveryResult> RecoverAsync(CancellationToken ct)
        {
            var ids = await _importJobsRepository.GetIdsByStatusAsync(
                                                  Domain.ImportJobStatus.Processing,
                                                  ct);

            if (ids.Count == 0)
            {
                return new ImportJobRecoveryResult(RecoveredJobs : 0,
                                                   DeletedFiles: 0,
                                                   DeletedRows: 0);             
            }

            var recoveredJobsCount = 0;
            var deletedFilesCount = 0;
            var deletedRowsCount = 0;
            
            foreach(var job in ids)
            {
                await _importJobsRepository.UpdateStatusAsync(job, 
                                                              ImportJobStatus.Failed, ct);

                var deletedRows = await _importRowsRepository.DeleteByJobIdAsync(job, ct);
                var fileDeleted =  _importFileStorage.DeleteIfExists(job);

                recoveredJobsCount++;
                deletedRowsCount += deletedRows;

                if (fileDeleted)
                {
                    deletedFilesCount++;
                }
            }

        _logger.LogInformation(
        "Startup import job recovery completed. Recovered jobs: {RecoveredJobs}, deleted staging rows: {DeletedRows}, deleted uploaded files: {DeletedFiles}.",
         recoveredJobsCount,
         deletedRowsCount,
         deletedFilesCount);
                                    
                                    
            return new ImportJobRecoveryResult(recoveredJobsCount, deletedRowsCount, deletedFilesCount);
        }
    }
}