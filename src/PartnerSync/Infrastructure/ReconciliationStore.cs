using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using PartnerSync.Contracts;
using PartnerSync.Contracts.Grpc;

namespace PartnerSync.Infrastructure;

public sealed class ReconciliationStore
{
    public async Task RecordAsync(NpgsqlConnection connection, string partnerId,
        PartnerOrder source, OrderSnapshot? target, CancellationToken ct)
    {
        var state = target is not null && target.SourceVersion > source.Version ? "conflict" :
            target is not null && target.Status == source.Status && target.TotalMinor == source.TotalMinor &&
            target.Currency == source.Currency && target.SourceVersion == source.Version ? "consistent" : "pending";
        var expectedRevision = target?.Revision ?? 0;
        var identity = JsonSerializer.Serialize(new { partnerId, source, expectedRevision }, WireJson.Options);
        var fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));

        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var observation = new NpgsqlCommand("""
            INSERT INTO sync.reconciliation
                (partner_id, order_id, state, source_version, target_revision, checked_at)
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (partner_id, order_id) DO UPDATE SET
                state = EXCLUDED.state, source_version = EXCLUDED.source_version,
                target_revision = EXCLUDED.target_revision, checked_at = now()
            """, connection, tx))
        {
            observation.Parameters.AddWithValue(partnerId);
            observation.Parameters.AddWithValue(source.OrderId);
            observation.Parameters.AddWithValue(state);
            observation.Parameters.AddWithValue(source.Version);
            observation.Parameters.AddWithValue(expectedRevision);
            await observation.ExecuteNonQueryAsync(ct);
        }

        if (state == "pending")
        {
            var message = new RepairRequested(Guid.NewGuid(), 1, DateTimeOffset.UtcNow,
                partnerId, source, expectedRevision);
            await using var insert = new NpgsqlCommand("""
                INSERT INTO sync.outbox (id, dedup_key, message_key, payload)
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (dedup_key) DO NOTHING
                """, connection, tx);
            insert.Parameters.AddWithValue(message.EventId);
            insert.Parameters.AddWithValue(fingerprint);
            insert.Parameters.AddWithValue($"{partnerId}:{source.OrderId}");
            insert.Parameters.AddWithValue(JsonSerializer.Serialize(message, WireJson.Options));
            await insert.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        Telemetry.Inspected.Add(1, new KeyValuePair<string, object?>("state", state));
    }
}
