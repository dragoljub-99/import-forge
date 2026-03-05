using ImportForge.Domain;

namespace ImportForge.Infrastructure.Repositories;

internal static class ImportJobStatusDbTokens
{
    public static string ToToken(ImportJobStatus status)
        => status switch
        {
            ImportJobStatus.Processing => "processing",
            ImportJobStatus.NeedsFixes => "needs_fixes",
            ImportJobStatus.Committed => "committed",
            ImportJobStatus.Failed => "failed",
            _ => throw new ArgumentOutOfRangeException(nameof(status), status, "Unknown import job status.")
        };

    public static ImportJobStatus FromToken(string token)
        => token switch
        {
            "processing" => ImportJobStatus.Processing,
            "needs_fixes" => ImportJobStatus.NeedsFixes,
            "committed" => ImportJobStatus.Committed,
            "failed" => ImportJobStatus.Failed,
            _ => throw new InvalidOperationException($"Unknown import job status token '{token}'.")
        };
}
