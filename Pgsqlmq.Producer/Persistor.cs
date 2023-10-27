using Dapper;
using Npgsql;
using Pgsqlmq.Messages;

namespace Pgsqlmq.Producer;

public sealed record PersistorOpts(
    NpgsqlDataSource Database,
    string NotificationChannel
);

public sealed class Persistor
{
    private readonly PersistorOpts _opts;

    public Persistor(PersistorOpts opts)
    {
        _opts = opts ?? throw new ArgumentNullException(nameof(opts));
    }

    public async Task Persist(Message message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        cancellationToken.ThrowIfCancellationRequested();

        await using var connection =
            await _opts.Database.OpenConnectionAsync(cancellationToken);

        await using var tx =
            await connection.BeginTransactionAsync(cancellationToken);
        
        Console.WriteLine("Adding a message");

        await connection.ExecuteAsync(
            @"INSERT INTO public.messages(Id, Payload) VALUES (@Id, @Payload);",
            new
            {
                message.Id,
                message.Payload
            },
            transaction: tx
        );

        Console.WriteLine("Sending a notification");
        
        await connection.ExecuteAsync(
            $@"NOTIFY {_opts.NotificationChannel};",
            transaction: tx
        );

        await tx.CommitAsync(cancellationToken);
    }
}
