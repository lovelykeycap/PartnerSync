using System.Security.Cryptography;
using System.Text;
using DemoSystems;
using Npgsql;
using PartnerSync.Contracts;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();
builder.Services.AddGrpc();
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("Postgres") ?? throw new InvalidOperationException("Postgres is required.")));
var partnerKey = builder.Configuration["PartnerApiKey"];
var internalKey = builder.Configuration["InternalApiKey"];
if (string.IsNullOrWhiteSpace(partnerKey) || string.IsNullOrWhiteSpace(internalKey))
    throw new InvalidOperationException("PartnerApiKey and InternalApiKey are required.");

var app = builder.Build();
app.Use(async (context, next) =>
{
    var grpc = context.Request.Path.StartsWithSegments("/partnersync.orders.v1.InternalOrders");
    var expected = grpc ? internalKey : partnerKey;
    if (!CryptographicOperations.FixedTimeEquals(
        SHA256.HashData(Encoding.UTF8.GetBytes(context.Request.Headers["X-Api-Key"].ToString())),
        SHA256.HashData(Encoding.UTF8.GetBytes(expected))))
    {
        if (grpc)
        {
            context.Response.ContentType = "application/grpc";
            context.Response.Headers["grpc-status"] = "16";
            context.Response.Headers["grpc-message"] = "Invalid API key";
        }
        else context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }
    await next(context);
});

// Keyset pagination: the cursor is the last order ID; status changes never reorder the result.
app.MapGet("/orders", async (NpgsqlDataSource db, string? cursor, int? limit, CancellationToken ct) =>
{
    var pageSize = Math.Clamp(limit ?? 100, 1, 100);
    if (cursor is { Length: > 100 }) return Results.BadRequest(new { error = "Invalid cursor" });
    await using var command = db.CreateCommand("""
        SELECT order_id, status, total_minor, currency, version
        FROM partner_demo.orders WHERE order_id COLLATE "C" > $1 COLLATE "C"
        ORDER BY order_id COLLATE "C" LIMIT $2
        """);
    command.Parameters.AddWithValue(cursor ?? "");
    command.Parameters.AddWithValue(pageSize + 1);
    var items = new List<PartnerOrder>();
    await using var reader = await command.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct)) items.Add(new PartnerOrder(reader.GetString(0), reader.GetString(1),
        reader.GetInt64(2), reader.GetString(3), reader.GetInt64(4)));
    var hasMore = items.Count > pageSize;
    if (hasMore) items.RemoveAt(items.Count - 1);
    return Results.Ok(new OrderPage(items, hasMore ? items[^1].OrderId : null));
});

app.MapPut("/demo/orders/{id}", async (string id, ChangeOrder change, NpgsqlDataSource db, CancellationToken ct) =>
{
    if (!OrderRules.IsValid(new PartnerOrder(id, change.Status, change.TotalMinor, change.Currency, 1)))
        return Results.BadRequest(new { error = "Invalid order" });
    await using var command = db.CreateCommand("""
        UPDATE partner_demo.orders SET status = $2, total_minor = $3, currency = $4, version = version + 1
        WHERE order_id = $1 RETURNING version
        """);
    command.Parameters.AddWithValue(id);
    command.Parameters.AddWithValue(change.Status);
    command.Parameters.AddWithValue(change.TotalMinor);
    command.Parameters.AddWithValue(change.Currency);
    var version = await command.ExecuteScalarAsync(ct);
    return version is null ? Results.NotFound() : Results.Ok(new { orderId = id, version });
});

app.MapGet("/demo/internal/orders", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var command = db.CreateCommand("""
        SELECT partner_id, order_id, status, total_minor, currency, source_version, revision
        FROM internal_system.orders ORDER BY partner_id, order_id LIMIT 200
        """);
    var items = new List<object>();
    await using var reader = await command.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct)) items.Add(new
    {
        partnerId = reader.GetString(0), orderId = reader.GetString(1), status = reader.GetString(2),
        totalMinor = reader.GetInt64(3), currency = reader.GetString(4),
        sourceVersion = reader.GetInt64(5), revision = reader.GetInt64(6)
    });
    return Results.Ok(items);
});

app.MapGrpcService<OrdersService>();
app.Run();

internal sealed record ChangeOrder(string Status, long TotalMinor, string Currency);
