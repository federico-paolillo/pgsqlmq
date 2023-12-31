using Dapper;
using Npgsql;
using Pgsqlmq.Messages;

namespace Pgsqlmq.Consumer;

public sealed record FetcherOpts(
    NpgsqlDataSource Database
);

public sealed class Fetcher
{
    private readonly FetcherOpts _opts;

    public Fetcher(FetcherOpts opts)
    {
        _opts = opts ?? throw new ArgumentNullException(nameof(opts));
    }

    public async Task<Message?> TryConsumeOne(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await using var connection =
            await _opts.Database.OpenConnectionAsync(cancellationToken);

        await using var tx =
            await connection.BeginTransactionAsync(cancellationToken);

        var maybeMessage = await connection.QueryFirstOrDefaultAsync<Message>(
            @"SELECT id, payload 
              FROM public.messages 
              ORDER BY id ASC
              FOR UPDATE SKIP LOCKED 
              LIMIT 1;",
            transaction: tx
        );

        if (maybeMessage is not null)
        {
            await connection.ExecuteAsync(
                @"DELETE FROM public.messages WHERE id = @Id",
                new
                {
                    maybeMessage.Id
                },
                transaction: tx
            );
        }

        await tx.CommitAsync(cancellationToken);

        return maybeMessage;
    }
}
