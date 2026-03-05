using ImportForge.Api.Contracts;
using ImportForge.Domain;
using ImportForge.Infrastructure.Db;
using ImportForge.Infrastructure.Repositories;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<DbConnectionFactory>();
builder.Services.AddScoped<DbInitializer>();
builder.Services.AddScoped<ImportJobsRepository>();
builder.Services.AddScoped<ImportRowErrorsRepository>();

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
