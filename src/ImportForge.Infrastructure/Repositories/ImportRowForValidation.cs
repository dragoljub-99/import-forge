namespace ImportForge.Infrastructure.Repositories;

public sealed record ImportRowForValidation(
    long Id,
    long JobId,
    int RowNumber,
    string? ProductId,
    string? ProductName,
    int? ProductRsdValue,
    int? ProductQuantity);
