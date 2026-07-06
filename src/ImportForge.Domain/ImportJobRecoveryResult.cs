namespace ImportForge.Domain
{
    public sealed record ImportJobRecoveryResult(int recoveredJobs,
                                                int DeletedFiles,
                                                int DeletedRows);
    
}