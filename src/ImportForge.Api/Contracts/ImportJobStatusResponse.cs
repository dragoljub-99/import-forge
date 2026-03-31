using ImportForge.Domain;

namespace ImportForge.Api.Contracts;

public sealed record ImportJobStatusResponse(
    long JobId,
    ImportJobStatus Status,
    int TotalRows,
    int ValidRows,
    int InvalidRows,
    string? ClearedAt,
    bool HasProblematicRows,
    int ProblematicRowsCount,
    string ProblematicRowsEndpoint);
