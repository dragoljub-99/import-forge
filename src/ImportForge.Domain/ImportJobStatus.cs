namespace ImportForge.Domain;

public enum ImportJobStatus
{
    Processing = 0,
    NeedsFixes = 1,
    Committed = 2,
    Failed = 3
}
