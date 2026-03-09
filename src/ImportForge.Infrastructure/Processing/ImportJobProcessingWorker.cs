using ImportForge.Domain;
using ImportForge.Infrastructure.Repositories;
using ImportForge.Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobProcessingWorker : BackgroundService
{
    private readonly ImportJobProcessingQueue _processingQueue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ImportJobProcessingWorker> _logger;

    public ImportJobProcessingWorker(
        ImportJobProcessingQueue processingQueue,
        IServiceScopeFactory scopeFactory,
        ILogger<ImportJobProcessingWorker> logger)
    {
        _processingQueue = processingQueue;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            long jobId;

            try
            {
                jobId = await _processingQueue.DequeueAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            _logger.LogInformation("Import job {JobId} dequeued. Processing started.", jobId);
            await ProcessJobAsync(jobId, stoppingToken);
        }
    }

    private async Task ProcessJobAsync(long jobId, CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var jobsRepository = scope.ServiceProvider.GetRequiredService<ImportJobsRepository>();
        var fileStorage = scope.ServiceProvider.GetRequiredService<ImportFileStorage>();

        try
        {
            var job = await jobsRepository.GetByIdAsync(jobId, ct);
            if (job is null)
            {
                _logger.LogWarning("Import job {JobId} was dequeued but does not exist.", jobId);
                return;
            }

            if (!fileStorage.Exists(jobId))
            {
                _logger.LogError("Import job {JobId} file is missing. Marking job as failed.", jobId);
                await TryUpdateStatusToFailedAsync(jobsRepository, jobId, ct);
                _logger.LogError("Import job {JobId} processing failed.", jobId);
                return;
            }

            await using var stream = fileStorage.OpenRead(jobId);
            if (!stream.CanRead)
            {
                _logger.LogError("Import job {JobId} file is not readable. Marking job as failed.", jobId);
                await TryUpdateStatusToFailedAsync(jobsRepository, jobId, ct);
                _logger.LogError("Import job {JobId} processing failed.", jobId);
                return;
            }

            _logger.LogInformation("Import job {JobId} processing finished.", jobId);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Import job {JobId} processing failed.", jobId);
            await TryUpdateStatusToFailedAsync(jobsRepository, jobId, ct);
        }
    }

    private async Task TryUpdateStatusToFailedAsync(ImportJobsRepository jobsRepository, long jobId, CancellationToken ct)
    {
        try
        {
            await jobsRepository.UpdateStatusAsync(jobId, ImportJobStatus.Failed, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update import job {JobId} status to failed.", jobId);
        }
    }
}
