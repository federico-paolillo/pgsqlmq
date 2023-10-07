using Dapper;
using Npgsql;

namespace Pgsqlmq.Consumer;

public sealed record ListenerOpts(
    NpgsqlDataSource Database,
    TimeSpan PollingInterval,
    TimeSpan NotificationWaitTime,
    TimeSpan MaxSilenceTime,
    string NotificationChannel);

/// <summary>
/// Listens for PostgreSQL notifications and delivers them
/// </summary>
public sealed class Listener
{
    private readonly ListenerOpts _opts;

    public Listener(ListenerOpts opts)
    {
        _opts = opts ?? throw new ArgumentNullException(nameof(opts));
    }

    public async Task WaitForNotification(
        CancellationToken cancellationToken = default)
    {
        // We poll but only for a finite amount of time
        // If, after a while, nothing comes we give up

        await Poll(cancellationToken)
            .WaitAsync(_opts.MaxSilenceTime, cancellationToken);
    }

    private async Task Poll(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var gotNotification = await Listen(cancellationToken);

            if (gotNotification)
            {
                return;
            }

            // Wait a bit before trying again
            // Prevents opening/closing connections in quick succession

            await Task.Delay(_opts.PollingInterval, cancellationToken);
        }
    }

    private async Task<bool> Listen(
        CancellationToken cancellationToken)
    {
        await using var connection =
            await _opts.Database.OpenConnectionAsync(cancellationToken);

        var notificationTsc = new TaskCompletionSource(
            null,
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        void OnNotification(object _,
            NpgsqlNotificationEventArgs args)
        {
            connection.Notification -= OnNotification;
            
            notificationTsc.SetResult();
        }

        connection.Notification += OnNotification;

        await connection.ExecuteAsync(
            @"LISTEN {NotificationChannel}",
            new
            {
                _opts.NotificationChannel
            }
        );

        var listenTimeout = Task.Delay(
            _opts.NotificationWaitTime,
            cancellationToken
        );

        var raceWinnerTask = await Task.WhenAny(
            listenTimeout,
            notificationTsc.Task
        );

        connection.Notification -= OnNotification;

        if (raceWinnerTask == listenTimeout)
        {
            return false;
        }

        return true;
    }
}
