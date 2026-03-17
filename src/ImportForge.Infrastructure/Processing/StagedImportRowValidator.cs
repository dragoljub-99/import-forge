using ImportForge.Infrastructure.Repositories;

namespace ImportForge.Infrastructure.Processing;

internal static class StagedImportRowValidator
{
    public const string ProductIdField = "ProductId";
    public const string ProductNameField = "ProductName";
    public const string ProductRsdValueField = "ProductRsdValue";
    public const string ProductQuantityField = "ProductQuantity";

    public const string ProductIdError = "ProductId must contain exactly 4 digits.";
    public const string ProductNameError = "ProductName is required.";
    public const string ProductRsdValueError = "ProductRsdValue must be a valid integer greater than 0.";
    public const string ProductQuantityError = "ProductQuantity must be a valid integer greater than or equal to 0.";

    public static IEnumerable<ImportRowFieldValidationError> Validate(ImportRowForValidation row)
    {
        if (!HasExactlyFourDigits(row.ProductId))
        {
            yield return new ImportRowFieldValidationError(ProductIdField, ProductIdError);
        }

        if (string.IsNullOrWhiteSpace(row.ProductName))
        {
            yield return new ImportRowFieldValidationError(ProductNameField, ProductNameError);
        }

        if (row.ProductRsdValue is null || row.ProductRsdValue <= 0)
        {
            yield return new ImportRowFieldValidationError(ProductRsdValueField, ProductRsdValueError);
        }

        if (row.ProductQuantity is null || row.ProductQuantity < 0)
        {
            yield return new ImportRowFieldValidationError(ProductQuantityField, ProductQuantityError);
        }
    }

    private static bool HasExactlyFourDigits(string? value)
    {
        if (value is null || value.Length != 4)
        {
            return false;
        }

        foreach (var character in value)
        {
            if (!char.IsDigit(character))
            {
                return false;
            }
        }

        return true;
    }
}
