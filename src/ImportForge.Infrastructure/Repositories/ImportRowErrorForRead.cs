namespace ImportForge.Infrastructure.Repositories;

public sealed record ImportRowErrorForRead(
    string Field,
    string Error);
