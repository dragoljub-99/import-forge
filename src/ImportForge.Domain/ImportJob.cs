namespace ImportForge.Domain;

public sealed record ImportJob(
    long Id,
    ImportJobStatus Status,
    int TotalRows,
    int ValidRows,
    int InvalidRows,
    DateTimeOffset? ClearedAt);
