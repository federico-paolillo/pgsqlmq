namespace Pgsqlmq.Consumer;

public sealed class Processor
{
    private readonly Listener _listener;
    private readonly Fetcher _fetcher;

    public Processor(Listener listener,
        Fetcher fetcher)
    {
        _listener = listener ??
                    throw new ArgumentNullException(nameof(listener));
        _fetcher = fetcher ?? throw new ArgumentNullException(nameof(fetcher));
    }

    public async Task Consume(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (!cancellationToken.IsCancellationRequested)
        {
            await _listener.WaitForNotification(cancellationToken);

            var message = await _fetcher.TryConsumeOne(cancellationToken);

            if (message is not null)
            {
                Console.WriteLine("Consumed message {0:N}", message.Id);
            }
        }
    }
}
