using ImportForge.Api.Contracts;
using ImportForge.Domain;
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
builder.Services.AddScoped<ImportRowErrorsRepository>();
builder.Services.AddSingleton<ImportFileStorage>();
builder.Services.AddSingleton<ImportJobProcessingQueue>();
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
        ImportRowErrorsRepository rowErrorsRepository,
        HttpContext httpContext) =>
    {
        var ct = httpContext.RequestAborted;

        var job = await jobsRepository.GetByIdAsync(jobId, ct);
        if (job is null)
        {
            return Results.NotFound();
        }

        var rowErrors = await rowErrorsRepository.ListByJobIdAsync(jobId, ct);
        var errorsByRow = rowErrors.Count == 0
            ? Array.Empty<RowErrorsDto>()
            : rowErrors
                .GroupBy(item => item.RowNumber)
                .Select(group => new RowErrorsDto(
                    group.Key,
                    group.Select(item => new ErrorDto(item.Field, item.Error)).ToArray()))
                .ToArray();

        var response = new ImportJobStatusResponse(
            job.Id,
            job.Status,
            job.TotalRows,
            job.ValidRows,
            job.InvalidRows,
            job.ClearedAt?.ToString("O"),
            errorsByRow);

        return Results.Ok(response);
    });

app.Run();
