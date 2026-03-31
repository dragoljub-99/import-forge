namespace ImportForge.Api.Contracts;

public sealed class RepairImportRowRequest
{
    private string? _productId;
    private string? _productName;
    private int? _productRsdValue;
    private int? _productQuantity;

    public bool HasProductId { get; private set; }
    public bool HasProductName { get; private set; }
    public bool HasProductRsdValue { get; private set; }
    public bool HasProductQuantity { get; private set; }

    public string? ProductId
    {
        get => _productId;
        set
        {
            HasProductId = true;
            _productId = value;
        }
    }

    public string? ProductName
    {
        get => _productName;
        set
        {
            HasProductName = true;
            _productName = value;
        }
    }

    public int? ProductRsdValue
    {
        get => _productRsdValue;
        set
        {
            HasProductRsdValue = true;
            _productRsdValue = value;
        }
    }

    public int? ProductQuantity
    {
        get => _productQuantity;
        set
        {
            HasProductQuantity = true;
            _productQuantity = value;
        }
    }
}
