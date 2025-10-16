using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;
using PartnerSync;
using PartnerSync.Contracts;
using PartnerSync.Contracts.Grpc;
using PartnerSync.Infrastructure;
using PartnerSync.Workers;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.Services.AddOptions<SyncSettings>().BindConfiguration("Sync")
    .Validate(s => s.IsValid(), "Invalid sync configuration.").ValidateOnStart();
builder.Services.AddOptions<KafkaSettings>().BindConfiguration("Kafka")
    .Validate(s => !string.IsNullOrWhiteSpace(s.BootstrapServers) &&
        !string.IsNullOrWhiteSpace(s.GroupId) && !string.IsNullOrWhiteSpace(s.RepairTopic) &&
        !string.IsNullOrWhiteSpace(s.DeadLetterTopic) && s.RepairTopic != s.DeadLetterTopic,
        "Invalid Kafka configuration.").ValidateOnStart();
var adminKey = builder.Configuration["AdminApiKey"];
if (string.IsNullOrWhiteSpace(adminKey)) throw new InvalidOperationException("AdminApiKey is required.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("Postgres") ?? throw new InvalidOperationException("Postgres is required.")));
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(
    builder.Configuration.GetConnectionString("Redis") ?? throw new InvalidOperationException("Redis is required.")));
builder.Services.AddSingleton<IProducer<string, string>>(sp =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = sp.GetRequiredService<IOptions<KafkaSettings>>().Value.BootstrapServers,
        EnableIdempotence = true,
        Acks = Acks.All,
        MessageTimeoutMs = 10_000,
        AllowAutoCreateTopics = false
    }).Build());
builder.Services.AddSingleton<IAdminClient>(sp => new AdminClientBuilder(new AdminClientConfig
{
    BootstrapServers = sp.GetRequiredService<IOptions<KafkaSettings>>().Value.BootstrapServers
}).Build());
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
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
builder.Services.AddHostedService<PollingWorker>();
builder.Services.AddHostedService<OutboxWorker>();
builder.Services.AddHostedService<RepairConsumer>();

var app = builder.Build();
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments("/api") &&
        !CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(context.Request.Headers["X-Api-Key"].ToString())),
            SHA256.HashData(Encoding.UTF8.GetBytes(adminKey))))
    {
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }
    await next(context);
});

app.MapGet("/health/live", () => Results.Ok(new { status = "alive" }));
app.MapGet("/health/ready", async (NpgsqlDataSource db, IConnectionMultiplexer redis,
    IAdminClient kafka, InternalOrdersClient internalOrders, IOptions<KafkaSettings> config,
    CancellationToken requestCt) =>
{
    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(requestCt);
    timeout.CancelAfter(TimeSpan.FromSeconds(5));
    try
    {
        await using var command = db.CreateCommand("SELECT 1");
        await command.ExecuteScalarAsync(timeout.Token);
        await redis.GetDatabase().PingAsync().WaitAsync(timeout.Token);
        var metadata = kafka.GetMetadata(TimeSpan.FromSeconds(2));
        if (new[] { config.Value.RepairTopic, config.Value.DeadLetterTopic }
            .Any(name => !metadata.Topics.Any(t => t.Topic == name && !t.Error.IsError)))
            return Results.StatusCode(503);
        await internalOrders.GetAsync("acme", "__readiness__", timeout.Token);
        return Results.Ok(new { status = "ready" });
    }
    catch (Exception) when (!requestCt.IsCancellationRequested) { return Results.StatusCode(503); }
});

app.MapGet("/api/reconciliation", async (NpgsqlDataSource db, int? limit, CancellationToken ct) =>
{
    await using var command = db.CreateCommand("""
        SELECT partner_id, order_id, state, source_version, target_revision, checked_at
        FROM sync.reconciliation ORDER BY checked_at DESC, partner_id, order_id LIMIT $1
        """);
    command.Parameters.AddWithValue(Math.Clamp(limit ?? 50, 1, 200));
    var rows = new List<object>();
    await using var reader = await command.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct)) rows.Add(new
    {
        partnerId = reader.GetString(0), orderId = reader.GetString(1), state = reader.GetString(2),
        sourceVersion = reader.GetInt64(3), targetRevision = reader.GetInt64(4), checkedAt = reader.GetDateTime(5)
    });
    return Results.Ok(rows);
});

app.MapGet("/api/outbox", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var command = db.CreateCommand("""
        SELECT count(*) FILTER (WHERE published_at IS NULL),
            count(*) FILTER (WHERE published_at IS NOT NULL), min(created_at) FILTER (WHERE published_at IS NULL)
        FROM sync.outbox
        """);
    await using var reader = await command.ExecuteReaderAsync(ct);
    await reader.ReadAsync(ct);
    return Results.Ok(new
    {
        pending = reader.GetInt64(0), published = reader.GetInt64(1),
        oldestPendingAt = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2)
    });
});

app.MapPost("/api/repairs/replay", async (ReplayRequest request, IProducer<string, string> producer,
    IOptions<KafkaSettings> kafka, CancellationToken ct) =>
{
    RepairRequested? message;
    try
    {
        message = JsonSerializer.Deserialize<RepairRequested>(request.OriginalPayload ?? "", WireJson.Options);
    }
    catch (JsonException) { return Results.BadRequest(new { error = "Invalid event JSON" }); }
    if (message is null || message.SchemaVersion != 1 || message.EventId == Guid.Empty ||
        string.IsNullOrWhiteSpace(message.PartnerId) || message.PartnerId.Length > 60 ||
        message.ExpectedRevision < 0 || !OrderRules.IsValid(message.Order))
        return Results.BadRequest(new { error = "Invalid repair event" });
    await producer.ProduceAsync(kafka.Value.RepairTopic, new Message<string, string>
    {
        Key = $"{message.PartnerId}:{message.Order.OrderId}",
        Value = JsonSerializer.Serialize(message, WireJson.Options)
    }, ct);
    return Results.Ok(new { message.EventId, status = "requeued" });
});
app.Run();

internal sealed record ReplayRequest(string OriginalPayload);
