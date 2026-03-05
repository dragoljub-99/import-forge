namespace ImportForge.Api.Contracts;

public sealed record RowErrorsDto(int RowNumber, IReadOnlyList<ErrorDto> Errors);
