namespace ImportForge.Infrastructure.Csv;

public sealed record CsvRowParseResult(
    int RowNumber,
    CsvRowParseKind Kind,
    IReadOnlyList<string> Columns);
