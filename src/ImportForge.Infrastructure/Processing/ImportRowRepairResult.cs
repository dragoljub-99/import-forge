namespace ImportForge.Infrastructure.Processing;

public enum ImportRowRepairResultStatus
{
    JobNotFound = 0,
    JobNotInNeedsFixes = 1,
    RowNotFound = 2,
    NoFieldsProvided = 3,
    StructuralRowRequiresFullPayload = 4,
    Repaired = 5
}

public sealed record ImportRowRepairResult(ImportRowRepairResultStatus Status);
