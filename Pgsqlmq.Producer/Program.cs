using Npgsql;
using Pgsqlmq.Producer;

using var sigintCancellationTokenSource = new CancellationTokenSource();

void OnCtrlC(object _,
    ConsoleCancelEventArgs args)
{
    args.Cancel = true;

    sigintCancellationTokenSource.Cancel();
}

Console.CancelKeyPress += OnCtrlC;

await using var database = NpgsqlDataSource.Create(
    "Host=localhost;Port=65432;Database=pgsqlmq;Username=postgres;Password=adminADMIN1234!"
);

var persistor = new Persistor(
    new PersistorOpts(
        Database: database,
        NotificationChannel: "pgsqlmq_notifications"
    )
);

var emitter = new Emitter(
    new EmitterOpts(
        EmissionInterval: TimeSpan.FromSeconds(1)
    ),
    persistor
);

await emitter.Emit(sigintCancellationTokenSource.Token);
