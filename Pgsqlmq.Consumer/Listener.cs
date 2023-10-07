using System.Threading.Channels;
using Dapper;
using Npgsql;

namespace Pgsqlmq.Consumer;

public sealed record ListenerOpts(
    NpgsqlDataSource Database,
    TimeSpan PollingInterval,
    TimeSpan MaxSilenceTime,
    int NotificationsBufferSize,
    string NotificationChannel);

public sealed record Notification;

/// <summary>
/// Listens for PostgreSQL notifications and delivers them
/// </summary>
public sealed class Listener
{
    private readonly ListenerOpts _opts;
    private readonly Channel<Notification> _channel;

    public Listener(ListenerOpts opts)
    {
        _opts = opts ?? throw new ArgumentNullException(nameof(opts));

        _channel = Channel.CreateBounded<Notification>(
            new BoundedChannelOptions(_opts.NotificationsBufferSize)
            {
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.DropOldest,
                Capacity = _opts.NotificationsBufferSize,
                SingleReader = false,
                SingleWriter = true
            }
        );
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        await Task.Run(
            () => Listen(cancellationToken),
            cancellationToken
        );
    }

    public async Task<Notification> WaitForNotification(
        CancellationToken cancellationToken)
    {
        return await _channel.Reader.ReadAsync(cancellationToken);
    }

    private async Task Listen(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (!cancellationToken.IsCancellationRequested)
        {
            var silenceTime = 0L;

            async void OnNotification(object _,
                NpgsqlNotificationEventArgs args)
            {
                silenceTime = 0L;

                await _channel.Writer.WriteAsync(
                    new Notification(),
                    cancellationToken
                );
            }

            await using var connection =
                await _opts.Database.OpenConnectionAsync(cancellationToken);

            connection.Notification += OnNotification;

            await connection.ExecuteAsync(
                @"LISTEN {NotificationChannel}",
                new
                {
                    _opts.NotificationChannel
                }
            );

            var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            while (!cancellationToken.IsCancellationRequested &&
                   silenceTime < _opts.MaxSilenceTime.TotalMilliseconds)
            {
                // Count for how long we loop
                // If something arrives the callback will reset silenceTime

                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                var delta = now - before;

                silenceTime = silenceTime + delta;
                before = now;

                // We are just waiting for something to come
                // We can surrender the execution to another thread

                await Task.Yield();
            }

            connection.Notification -= OnNotification;

            // Wait a bit before trying again

            await Task.Delay(_opts.PollingInterval, cancellationToken);
        }
    }
}
