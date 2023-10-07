using Npgsql;
using Pgsqlmq.Consumer;

using var sigintCancellationTokenSource = new CancellationTokenSource();

void OnCtrlC(object _,
    ConsoleCancelEventArgs args)
{
    args.Cancel = true;

    sigintCancellationTokenSource.Cancel();
}

Console.CancelKeyPress += OnCtrlC;

await using var database = NpgsqlDataSource.Create(
    "Host=localhost;Port=65535;Database=pgsqlmq;Username=postgres;Password=adminADMIN1234!"
);

var listener = new Listener(
    new ListenerOpts(
        Database: database,
        NotificationChannel: "pgsqlmq_notifications",
        PollingInterval: TimeSpan.FromMinutes(1),
        MaxSilenceTime: TimeSpan.FromSeconds(30),
        NotificationsBufferSize: 64
    )
);

var fetcher = new Fetcher(
    new FetcherOpts
    (
        Database: database
    )
);

var processor = new Processor(listener, fetcher);

var listeningTask = listener.Start(sigintCancellationTokenSource.Token);
var processingTask = processor.Consume(sigintCancellationTokenSource.Token);

await Task.WhenAll(listeningTask, processingTask);

await listeningTask;
await processingTask;
