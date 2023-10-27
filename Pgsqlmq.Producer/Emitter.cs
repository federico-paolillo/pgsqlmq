using Npgsql;
using Pgsqlmq.Consumer;
using Pgsqlmq.Messages;

namespace Pgsqlmq.Producer;

public sealed record EmitterOpts(
    TimeSpan EmissionInterval
);

public sealed class Emitter
{
    private readonly EmitterOpts _opts;
    private readonly Persistor _persistor;

    public Emitter(EmitterOpts opts,
        Persistor persistor)

    {
        _opts = opts ?? throw new ArgumentNullException(nameof(opts));
        _persistor = persistor ??
                     throw new ArgumentNullException(nameof(persistor));
    }

    public async Task Emit(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (!cancellationToken.IsCancellationRequested)
        {
            var message = Message.New();

            await _persistor.Persist(message, cancellationToken);

            await Task.Delay(_opts.EmissionInterval, cancellationToken);
        }
    }
}
