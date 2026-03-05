namespace ImportForge.Infrastructure.Repositories;

public sealed record ImportJobErrorItem(int RowNumber, string Field, string Error);
