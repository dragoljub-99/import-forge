namespace ImportForge.Domain
{
    public sealed record ImportJobRecoveryResult(int RecoveredJobs,
                                                int DeletedRows,
                                                int DeletedFiles);
    
}