using ImportForge.Api.Contracts;
using ImportForge.Domain;
using ImportForge.Infrastructure.Csv;
using ImportForge.Infrastructure.Db;
using ImportForge.Infrastructure.Processing;
using ImportForge.Infrastructure.Repositories;
using ImportForge.Infrastructure.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<DbConnectionFactory>();
builder.Services.AddScoped<DbInitializer>();
builder.Services.AddScoped<ImportJobsRepository>();
builder.Services.AddScoped<ImportRowsRepository>();
builder.Services.AddScoped<ImportRowErrorsRepository>();
builder.Services.AddScoped<ImportJobAutoCommitService>();
builder.Services.AddScoped<ImportRowRepairService>();
builder.Services.AddSingleton<StreamingCsvParser>();
builder.Services.AddSingleton<ImportFileStorage>();
builder.Services.AddSingleton<ImportJobProcessingQueue>();
builder.Services.AddSingleton<ImportJobProcessingGuard>();
builder.Services.AddHostedService<ImportJobProcessingWorker>();

var app = builder.Build();

using (var scope = app.Services.CreateAsyncScope())
{
    var dbInitializer = scope.ServiceProvider.GetRequiredService<DbInitializer>();
    await dbInitializer.InitializeAsync(app.Lifetime.ApplicationStopping);
}

if (app.Environment.IsDevelopment()) 
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

if (!app.Environment.IsDevelopment())
{
    app.UseHttpsRedirection();
}


app.MapGet("/health", () => Results.Text("OK", "text/plain"));

app.MapPost(
    "/import-jobs",
    async (
        IFormFile? file,
        ImportJobsRepository jobsRepository,
        ImportFileStorage fileStorage,
        ImportJobProcessingQueue processingQueue,
        ILogger<Program> logger,
        HttpContext httpContext) =>
    {
        if (file is null)
        {
            return Results.BadRequest(new { message = "File is required." });
        }

        if (file.Length == 0)
        {
            return Results.BadRequest(new { message = "File must not be empty." });
        }

        var ct = httpContext.RequestAborted;
        var jobId = await jobsRepository.CreateAsync(ImportJobStatus.Processing, ct);

        try
        {
            await using var stream = file.OpenReadStream();
            await fileStorage.SaveAsync(jobId, stream, ct);
        }
        catch
        {
            try
            {
                await jobsRepository.UpdateStatusAsync(jobId, ImportJobStatus.Failed, ct);
            }
            catch
            {
            }

            return Results.Problem(
                title: "Failed to save uploaded file.",
                statusCode: StatusCodes.Status500InternalServerError);
        }

        try
        {
            await processingQueue.EnqueueAsync(jobId, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to enqueue import job {JobId} for background processing.", jobId);

            try
            {
                await jobsRepository.UpdateStatusAsync(jobId, ImportJobStatus.Failed, ct);
            }
            catch (Exception updateException)
            {
                logger.LogError(updateException, "Failed to mark import job {JobId} as failed after queueing error.", jobId);
            }

            return Results.Problem(
                title: "File was uploaded, but the job could not be queued for processing.",
                statusCode: StatusCodes.Status500InternalServerError);
        }

        return Results.Accepted($"/import-jobs/{jobId}", new { jobId });
    }).DisableAntiforgery();

app.MapGet(
    "/import-jobs/{jobId:long}",
    async (
        long jobId,
        ImportJobsRepository jobsRepository,
        HttpContext httpContext) =>
    {
        var ct = httpContext.RequestAborted;

        var job = await jobsRepository.GetByIdAsync(jobId, ct);
        if (job is null)
        {
            return Results.NotFound();
        }

        var problematicRowsCount = job.InvalidRows;

        var response = new ImportJobStatusResponse(
            job.Id,
            job.Status,
            job.TotalRows,
            job.ValidRows,
            job.InvalidRows,
            job.ClearedAt?.ToString("O"),
            problematicRowsCount > 0,
            problematicRowsCount,
            $"/import-jobs/{jobId}/problematic-rows");

        return Results.Ok(response);
    });

app.MapGet(
    "/import-jobs/{jobId:long}/problematic-rows",
    async (
        long jobId,
        ImportJobsRepository jobsRepository,
        ImportRowsRepository rowsRepository,
        ImportRowErrorsRepository rowErrorsRepository,
        HttpContext httpContext) =>
    {
        var ct = httpContext.RequestAborted;

        var job = await jobsRepository.GetByIdAsync(jobId, ct);
        if (job is null)
        {
            return Results.NotFound();
        }

        var problematicRows = await rowsRepository.ListProblematicByJobIdAsync(jobId, ct);
        var rowErrors = await rowErrorsRepository.ListByJobIdAsync(jobId, ct);

        var errorsByRow = rowErrors
            .GroupBy(item => item.RowNumber)
            .ToDictionary(
                group => group.Key,
                group => (IReadOnlyList<ErrorDto>)group
                    .Select(item => new ErrorDto(item.Field, item.Error))
                    .ToArray());

        var rows = problematicRows
            .Select(row =>
                new ProblematicRowDto(
                    row.RowNumber,
                    row.SourceRaw,
                    row.SourceColumnCount,
                    row.ProductId,
                    row.ProductName,
                    row.ProductRsdValue,
                    row.ProductQuantity,
                    errorsByRow.TryGetValue(row.RowNumber, out var errors) ? errors : Array.Empty<ErrorDto>()))
            .ToArray();

        var response = new ImportJobProblematicRowsResponse(
            job.Id,
            rows.Length,
            rows);

        return Results.Ok(response);
    });

app.MapPatch(
    "/import-jobs/{jobId:long}/rows/{rowNumber:int}",
    async (
        long jobId,
        int rowNumber,
        RepairImportRowRequest? request,
        ImportRowRepairService rowRepairService,
        HttpContext httpContext) =>
    {
        if (request is null)
        {
            return Results.BadRequest(new { message = "Request body is required." });
        }

        var patch = new StagedImportRowPatch(
            request.HasProductId,
            request.ProductId,
            request.HasProductName,
            request.ProductName,
            request.HasProductRsdValue,
            request.ProductRsdValue,
            request.HasProductQuantity,
            request.ProductQuantity);

        var result = await rowRepairService.RepairAsync(jobId, rowNumber, patch, httpContext.RequestAborted);

        return result.Status switch
        {
            ImportRowRepairResultStatus.JobNotFound => Results.NotFound(),
            ImportRowRepairResultStatus.JobNotInNeedsFixes => Results.Conflict(
                new { message = "Only jobs in NeedsFixes can be repaired." }),
            ImportRowRepairResultStatus.RowNotFound => Results.NotFound(),
            ImportRowRepairResultStatus.NoFieldsProvided => Results.BadRequest(
                new { message = "At least one business field must be provided." }),
            ImportRowRepairResultStatus.StructuralRowRequiresFullPayload => Results.BadRequest(
                new { message = "Rows with structural errors require all business fields: ProductId, ProductName, ProductRsdValue, ProductQuantity." }),
            ImportRowRepairResultStatus.Repaired => Results.NoContent(),
            _ => Results.Problem(statusCode: StatusCodes.Status500InternalServerError)
        };
    });

app.Run();
