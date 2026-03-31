namespace ImportForge.Infrastructure.Repositories;

public sealed record ImportProblematicRowForRead(
    int RowNumber,
    string? SourceRaw,
    int? SourceColumnCount,
    string? ProductId,
    string? ProductName,
    int? ProductRsdValue,
    int? ProductQuantity);
