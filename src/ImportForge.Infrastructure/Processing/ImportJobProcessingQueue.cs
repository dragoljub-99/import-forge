using System.Threading.Channels;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobProcessingQueue
{
    private readonly Channel<long> _jobIdsChannel;

    public ImportJobProcessingQueue()
    {
        _jobIdsChannel = Channel.CreateUnbounded<long>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public ValueTask EnqueueAsync(long jobId, CancellationToken ct)
        => _jobIdsChannel.Writer.WriteAsync(jobId, ct);

    public ValueTask<long> DequeueAsync(CancellationToken ct)
        => _jobIdsChannel.Reader.ReadAsync(ct);
}
