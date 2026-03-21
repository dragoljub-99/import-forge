using System.Collections.Concurrent;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobProcessingGuard
{
    private readonly ConcurrentDictionary<long, byte> _processingJobIds = new();

    public bool TryAcquire(long jobId)
        => _processingJobIds.TryAdd(jobId, default);

    public bool Release(long jobId)
        => _processingJobIds.TryRemove(jobId, out _);
}
