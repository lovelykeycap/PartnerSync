using Grpc.Core;
using Google.Protobuf;
using Npgsql;
using PartnerSync.Contracts;
using PartnerSync.Contracts.Grpc;

namespace DemoSystems;

public sealed class OrdersService(NpgsqlDataSource database) : InternalOrders.InternalOrdersBase
{
    public override async Task<GetOrderReply> GetOrder(GetOrderRequest request, ServerCallContext context)
    {
        ValidateKey(request.PartnerId, request.OrderId);
        await using var command = database.CreateCommand("""
            SELECT status, total_minor, currency, source_version, revision
            FROM internal_system.orders WHERE partner_id = $1 AND order_id = $2
            """);
        command.Parameters.AddWithValue(request.PartnerId);
        command.Parameters.AddWithValue(request.OrderId);
        await using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return new GetOrderReply { Found = false };
        return new GetOrderReply
        {
            Found = true,
            Order = new OrderSnapshot
            {
                PartnerId = request.PartnerId, OrderId = request.OrderId,
                Status = reader.GetString(0), TotalMinor = reader.GetInt64(1), Currency = reader.GetString(2),
                SourceVersion = reader.GetInt64(3), Revision = reader.GetInt64(4)
            }
        };
    }

    public override async Task<ApplyPartnerOrderReply> ApplyPartnerOrder(
        ApplyPartnerOrderRequest request, ServerCallContext context)
    {
        var order = request.Order;
        if (order is null || !Guid.TryParse(request.EventId, out var eventId) || eventId == Guid.Empty ||
            request.ExpectedRevision < 0 || !OrderRules.IsValid(new PartnerOrder(order.OrderId,
                order.Status, order.TotalMinor, order.Currency, order.SourceVersion)))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid repair request."));
        ValidateKey(order.PartnerId, order.OrderId);
        var ct = context.CancellationToken;
        var requestHash = System.Security.Cryptography.SHA256.HashData(request.ToByteArray());
        await using var connection = await database.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        // Serializes duplicate event IDs, then orders, including creation of a missing order.
        await LockAsync($"event:{eventId}");
        await using (var inbox = new NpgsqlCommand(
            "SELECT outcome, request_hash FROM internal_system.inbox WHERE event_id = $1", connection, tx))
        {
            inbox.Parameters.AddWithValue(eventId);
            await using var reader = await inbox.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                if (!System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
                    reader.GetFieldValue<byte[]>(1), requestHash))
                    throw new RpcException(new Status(StatusCode.InvalidArgument, "Event ID was reused with a different payload."));
                return new ApplyPartnerOrderReply { Outcome = (ApplyOutcome)reader.GetInt32(0) };
            }
        }

        await LockAsync($"order:{order.PartnerId}:{order.OrderId}");
        long revision = 0, sourceVersion = 0;
        await using (var select = new NpgsqlCommand("""
            SELECT revision, source_version FROM internal_system.orders
            WHERE partner_id = $1 AND order_id = $2 FOR UPDATE
            """, connection, tx))
        {
            select.Parameters.AddWithValue(order.PartnerId);
            select.Parameters.AddWithValue(order.OrderId);
            await using var reader = await select.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                revision = reader.GetInt64(0);
                sourceVersion = reader.GetInt64(1);
            }
        }

        var outcome = sourceVersion > order.SourceVersion ? 3 :
            revision != request.ExpectedRevision ? 2 : 1;
        if (outcome == 1)
        {
            await using var upsert = new NpgsqlCommand("""
                INSERT INTO internal_system.orders
                    (partner_id, order_id, status, total_minor, currency, source_version, revision)
                VALUES ($1, $2, $3, $4, $5, $6, 1)
                ON CONFLICT (partner_id, order_id) DO UPDATE SET
                    status = EXCLUDED.status, total_minor = EXCLUDED.total_minor,
                    currency = EXCLUDED.currency, source_version = EXCLUDED.source_version,
                    revision = internal_system.orders.revision + 1
                """, connection, tx);
            upsert.Parameters.AddWithValue(order.PartnerId);
            upsert.Parameters.AddWithValue(order.OrderId);
            upsert.Parameters.AddWithValue(order.Status);
            upsert.Parameters.AddWithValue(order.TotalMinor);
            upsert.Parameters.AddWithValue(order.Currency);
            upsert.Parameters.AddWithValue(order.SourceVersion);
            await upsert.ExecuteNonQueryAsync(ct);
        }

        await using var remember = new NpgsqlCommand("""
            INSERT INTO internal_system.inbox (event_id, outcome, request_hash) VALUES ($1, $2, $3)
            """, connection, tx);
        remember.Parameters.AddWithValue(eventId);
        remember.Parameters.AddWithValue(outcome);
        remember.Parameters.AddWithValue(requestHash);
        await remember.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
        return new ApplyPartnerOrderReply { Outcome = (ApplyOutcome)outcome };

        async Task LockAsync(string key)
        {
            await using var command = new NpgsqlCommand(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", connection, tx);
            command.Parameters.AddWithValue(key);
            await command.ExecuteScalarAsync(ct);
        }
    }

    private static void ValidateKey(string partnerId, string orderId)
    {
        if (string.IsNullOrWhiteSpace(partnerId) || partnerId.Length > 60 ||
            string.IsNullOrWhiteSpace(orderId) || orderId.Length > 100)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid order key."));
    }
}
