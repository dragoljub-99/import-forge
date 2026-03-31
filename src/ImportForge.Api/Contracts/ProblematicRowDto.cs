namespace ImportForge.Api.Contracts;

public sealed record ProblematicRowDto(
    int RowNumber,
    string? SourceRaw,
    int? SourceColumnCount,
    string? ProductId,
    string? ProductName,
    int? ProductRsdValue,
    int? ProductQuantity,
    IReadOnlyList<ErrorDto> Errors);
