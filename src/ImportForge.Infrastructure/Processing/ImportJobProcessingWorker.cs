using System.Globalization;
using ImportForge.Infrastructure.Csv;
using ImportForge.Domain;
using ImportForge.Infrastructure.Repositories;
using ImportForge.Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobProcessingWorker : BackgroundService
{
    private const int ExpectedColumnCount = 4;
    private const string UnknownFieldName = "unknown";
    private const string WrongColumnCountError = "Wrong column count.";
    private const string MalformedLineError = "Malformed CSV row.";

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
        var rowsRepository = scope.ServiceProvider.GetRequiredService<ImportRowsRepository>();
        var rowErrorsRepository = scope.ServiceProvider.GetRequiredService<ImportRowErrorsRepository>();
        var csvParser = scope.ServiceProvider.GetRequiredService<StreamingCsvParser>();
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

            var totalRows = await ParseAndStageRowsAsync(
                jobId,
                stream,
                csvParser,
                rowsRepository,
                rowErrorsRepository,
                ct);

            await ValidateStagedRowsAsync(
                jobId,
                rowsRepository,
                rowErrorsRepository,
                ct);

            var invalidRows = await rowErrorsRepository.CountDistinctRowsWithErrorsByJobIdAsync(jobId, ct);
            var validRows = totalRows - invalidRows;

            await jobsRepository.UpdateCountersAsync(jobId, totalRows, validRows, invalidRows, ct);

            if (invalidRows > 0)
            {
                await jobsRepository.UpdateStatusAsync(jobId, ImportJobStatus.NeedsFixes, ct);
            }

            _logger.LogInformation(
                "Import job {JobId} processing finished. Parsed {TotalRows} row(s), valid {ValidRows}, invalid {InvalidRows}.",
                jobId,
                totalRows,
                validRows,
                invalidRows);
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

    private static async Task<int> ParseAndStageRowsAsync(
        long jobId,
        Stream stream,
        StreamingCsvParser csvParser,
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository,
        CancellationToken ct)
    {
        var totalRows = 0;

        await foreach (var parsedRow in csvParser.ParseAsync(stream, ct))
        {
            totalRows = parsedRow.RowNumber;

            if (parsedRow.Kind == CsvRowParseKind.Malformed)
            {
                await StageStructuralErrorAsync(
                    rowsRepository,
                    rowErrorsRepository,
                    jobId,
                    parsedRow.RowNumber,
                    MalformedLineError,
                    ct);
                continue;
            }

            if (parsedRow.Columns.Count != ExpectedColumnCount)
            {
                await StageStructuralErrorAsync(
                    rowsRepository,
                    rowErrorsRepository,
                    jobId,
                    parsedRow.RowNumber,
                    WrongColumnCountError,
                    ct);
                continue;
            }

            var stagedRow = new ImportRowForInsert(
                jobId,
                parsedRow.RowNumber,
                ToNullableText(parsedRow.Columns[0]),
                ToNullableText(parsedRow.Columns[1]),
                ToNullableInt(parsedRow.Columns[2]),
                ToNullableInt(parsedRow.Columns[3]));

            await rowsRepository.AddAsync(stagedRow, ct);
        }

        return totalRows;
    }

    private static async Task ValidateStagedRowsAsync(
        long jobId,
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository,
        CancellationToken ct)
    {
        await rowErrorsRepository.DeleteFieldLevelByJobIdAsync(jobId, ct);
        var stagedRows = await rowsRepository.ListValidatableByJobIdAsync(jobId, ct);

        foreach (var stagedRow in stagedRows)
        {
            foreach (var validationError in StagedImportRowValidator.Validate(stagedRow))
            {
                await rowErrorsRepository.AddAsync(stagedRow.Id, validationError.Field, validationError.Error, ct);
            }
        }
    }

    private static async Task StageStructuralErrorAsync(
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository,
        long jobId,
        int rowNumber,
        string error,
        CancellationToken ct)
    {
        var rowId = await rowsRepository.AddAsync(
            new ImportRowForInsert(
                jobId,
                rowNumber,
                ProductId: null,
                ProductName: null,
                ProductRsdValue: null,
                ProductQuantity: null),
            ct);

        await rowErrorsRepository.AddAsync(rowId, UnknownFieldName, error, ct);
    }

    private static string? ToNullableText(string value)
        => value.Length == 0 ? null : value;

    private static int? ToNullableInt(string value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return null;
        }

        return parsed;
    }
}
