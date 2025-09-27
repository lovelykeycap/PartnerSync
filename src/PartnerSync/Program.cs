using Microsoft.Extensions.Options;
using Npgsql;
using PartnerSync;
using PartnerSync.Contracts.Grpc;
using PartnerSync.Infrastructure;
using PartnerSync.Workers;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.Services.AddOptions<SyncSettings>().BindConfiguration("Sync")
    .Validate(s => s.IsValid(), "Invalid sync configuration.").ValidateOnStart();
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("Postgres") ?? throw new InvalidOperationException("Postgres is required.")));
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(
    builder.Configuration.GetConnectionString("Redis") ?? throw new InvalidOperationException("Redis is required.")));
builder.Services.AddHttpClient("partners", client =>
    {
        client.Timeout = Timeout.InfiniteTimeSpan;
        client.MaxResponseContentBufferSize = 1_048_576;
    })
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        AllowAutoRedirect = false,
        MaxResponseHeadersLength = 32,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5)
    });
builder.Services.AddGrpcClient<InternalOrders.InternalOrdersClient>((sp, options) =>
    options.Address = new Uri(sp.GetRequiredService<IOptions<SyncSettings>>().Value.InternalGrpcUrl));
builder.Services.AddSingleton<PartnerClient>();
builder.Services.AddSingleton<InternalOrdersClient>();
builder.Services.AddSingleton<ReconciliationStore>();
builder.Services.AddHostedService<PollingWorker>();

var app = builder.Build();
app.MapGet("/health/live", () => Results.Ok(new { status = "alive" }));
app.Run();
