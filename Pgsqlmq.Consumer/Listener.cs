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
        cancellationToken.ThrowIfCancellationRequested();

        await Task.Run(
            () => ListenSafely(cancellationToken),
            cancellationToken
        );
    }

    public async Task<Notification> WaitForNotification(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return await _channel.Reader.ReadAsync(cancellationToken);
    }

    private async Task ListenSafely(CancellationToken cancellationToken)
    {
        try
        {
            await Listen(cancellationToken);
        }
        catch (Exception ex)
        {
            _channel.Writer.Complete(ex);

            throw;
        }
    }

    private async Task Listen(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (!cancellationToken.IsCancellationRequested)
        {
            var gotNotification = false;

            async void OnNotification(object _,
                NpgsqlNotificationEventArgs args)
            {
                Console.WriteLine("Got a notification!");

                gotNotification = true;

                await _channel.Writer.WriteAsync(
                    new Notification(),
                    cancellationToken
                );
            }

            await using var connection =
                await _opts.Database.OpenConnectionAsync(cancellationToken);

            connection.Notification += OnNotification;

            using var silenceTimer = new PeriodicTimer(_opts.MaxSilenceTime);

            await connection.ExecuteAsync(
                @$"LISTEN {_opts.NotificationChannel};"
            );

            Console.WriteLine("Listening for notifications");

            while (!cancellationToken.IsCancellationRequested)
            {
                await silenceTimer.WaitForNextTickAsync(cancellationToken);

                if (gotNotification)
                {
                    Console.WriteLine("Notifications keep on coming in");

                    gotNotification = false;
                }
                else
                {
                    Console.WriteLine(
                        "There were no notification for too long."
                    );

                    break;
                }
            }

            connection.Notification -= OnNotification;

            // Release connection

            await connection.CloseAsync();

            // Wait a bit before trying again

            Console.WriteLine(
                "Returned connection. Going to sleep, will try to listen again later"
            );

            await Task.Delay(_opts.PollingInterval, cancellationToken);
        }
    }
}
