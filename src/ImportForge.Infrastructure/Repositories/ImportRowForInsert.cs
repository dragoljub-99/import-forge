namespace ImportForge.Infrastructure.Repositories;

public sealed record ImportRowForInsert(
    long JobId,
    int RowNumber,
    string? ProductId,
    string? ProductName,
    int? ProductRsdValue,
    int? ProductQuantity);
