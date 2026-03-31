namespace ImportForge.Infrastructure.Processing;

public sealed record StagedImportRowPatch(
    bool HasProductId,
    string? ProductId,
    bool HasProductName,
    string? ProductName,
    bool HasProductRsdValue,
    int? ProductRsdValue,
    bool HasProductQuantity,
    int? ProductQuantity)
{
    public bool HasAnyField
        => HasProductId
            || HasProductName
            || HasProductRsdValue
            || HasProductQuantity;

    public bool HasAllBusinessFields
        => HasProductId
            && HasProductName
            && HasProductRsdValue
            && HasProductQuantity;
}
