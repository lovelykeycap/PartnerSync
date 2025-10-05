using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace PartnerSync.Workers;

public sealed class OutboxWorker(
    NpgsqlDataSource database,
    IProducer<string, string> producer,
    IOptions<KafkaSettings> settings,
    ILogger<OutboxWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await PublishOneAsync(stoppingToken))
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox iteration failed");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private async Task<bool> PublishOneAsync(CancellationToken ct)
    {
        await using var connection = await database.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        Guid id;
        string key, payload;
        int attempts;
        await using (var select = new NpgsqlCommand("""
            SELECT id, message_key, payload::text, attempts FROM sync.outbox
            WHERE published_at IS NULL AND next_attempt_at <= now()
            ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
            """, connection, tx))
        await using (var reader = await select.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct)) return false;
            id = reader.GetGuid(0);
            key = reader.GetString(1);
            payload = reader.GetString(2);
            attempts = reader.GetInt32(3);
        }

        try
        {
            // Keep the row lock through the broker acknowledgement; delivery timeout is bounded.
            await producer.ProduceAsync(settings.Value.RepairTopic,
                new Message<string, string> { Key = key, Value = payload }, ct);
        }
        catch (ProduceException<string, string> ex)
        {
            await using var retry = new NpgsqlCommand("""
                UPDATE sync.outbox SET attempts = attempts + 1, last_error = $2,
                    next_attempt_at = now() + make_interval(secs => $3) WHERE id = $1
                """, connection, tx);
            retry.Parameters.AddWithValue(id);
            retry.Parameters.AddWithValue(ex.Error.Code.ToString());
            retry.Parameters.AddWithValue(Math.Min(60d, Math.Pow(2, Math.Min(attempts, 6))));
            await retry.ExecuteNonQueryAsync(ct);
            await tx.CommitAsync(ct);
            logger.LogWarning("Outbox {EventId}: Kafka error {ErrorCode}; retry scheduled", id, ex.Error.Code);
            return true;
        }

        await using var mark = new NpgsqlCommand(
            "UPDATE sync.outbox SET published_at = now(), last_error = NULL WHERE id = $1", connection, tx);
        mark.Parameters.AddWithValue(id);
        await mark.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
        Telemetry.Published.Add(1);
        return true;
    }
}
