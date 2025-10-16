using System.Text.Json;
using Confluent.Kafka;
using Grpc.Core;
using Microsoft.Extensions.Options;
using PartnerSync.Contracts;
using PartnerSync.Infrastructure;

namespace PartnerSync.Workers;

public sealed class RepairConsumer(
    IProducer<string, string> producer,
    InternalOrdersClient internalOrders,
    IOptions<KafkaSettings> kafka,
    IOptions<SyncSettings> settings,
    ILogger<RepairConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = kafka.Value.BootstrapServers,
            GroupId = kafka.Value.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            MaxPollIntervalMs = 300_000,
            AllowAutoCreateTopics = false
        }).Build();
        consumer.Subscribe(kafka.Value.RepairTopic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> record;
                try { record = consumer.Consume(stoppingToken); }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    logger.LogWarning("Kafka consume error {ErrorCode}", ex.Error.Code);
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    continue;
                }

                // A failure to publish a DLQ record escapes this method and stops the host.
                // Advancing to the next message here would risk committing past lost work.
                await HandleAsync(record, stoppingToken);
                try { consumer.Commit(record); }
                catch (KafkaException ex)
                {
                    // Work is durable already. A rebalance can redeliver it safely.
                    logger.LogWarning("Could not commit offset {Offset}: {ErrorCode}",
                        record.TopicPartitionOffset, ex.Error.Code);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally { consumer.Close(); }
    }

    private async Task HandleAsync(ConsumeResult<string, string> record, CancellationToken ct)
    {
        RepairRequested? message;
        try
        {
            message = JsonSerializer.Deserialize<RepairRequested>(record.Message.Value ?? "", WireJson.Options);
        }
        catch (JsonException)
        {
            await DeadLetterAsync(record, "invalid_json", ct);
            return;
        }

        if (message is null || message.SchemaVersion != 1 || message.EventId == Guid.Empty ||
            string.IsNullOrWhiteSpace(message.PartnerId) || message.PartnerId.Length > 60 ||
            message.ExpectedRevision < 0 || !OrderRules.IsValid(message.Order) ||
            record.Message.Key != $"{message.PartnerId}:{message.Order.OrderId}")
        {
            await DeadLetterAsync(record, "invalid_contract", ct);
            return;
        }

        using var activity = Telemetry.Activities.StartActivity("repair.apply");
        activity?.SetTag("event.id", message.EventId);
        for (var attempt = 1; attempt <= settings.Value.MaxApplyAttempts; attempt++)
        {
            try
            {
                var outcome = await internalOrders.ApplyAsync(message, ct);
                if ((int)outcome == 1) Telemetry.Applied.Add(1);
                logger.LogInformation("Repair {EventId} for {PartnerId}/{OrderId}: {Outcome}",
                    message.EventId, message.PartnerId, message.Order.OrderId, outcome);
                return; // Conflicts and stale versions are re-evaluated by the next sweep.
            }
            catch (RpcException ex) when (ct.IsCancellationRequested && ex.StatusCode == StatusCode.Cancelled)
            {
                throw new OperationCanceledException(ct);
            }
            catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or
                StatusCode.DeadlineExceeded or StatusCode.ResourceExhausted or StatusCode.Aborted)
            {
                if (attempt == settings.Value.MaxApplyAttempts)
                {
                    await DeadLetterAsync(record, $"grpc_retries_exhausted:{ex.StatusCode}", ct);
                    return;
                }
                await Task.Delay(TimeSpan.FromMilliseconds(
                    Math.Min(10_000, 500 * Math.Pow(2, attempt - 1)) + Random.Shared.Next(200)), ct);
            }
            catch (RpcException ex)
            {
                await DeadLetterAsync(record, $"grpc_permanent:{ex.StatusCode}", ct);
                return;
            }
            catch (InvalidDataException)
            {
                await DeadLetterAsync(record, "invalid_grpc_response", ct);
                return;
            }
        }
    }

    private async Task DeadLetterAsync(ConsumeResult<string, string> record, string reason, CancellationToken ct)
    {
        var deadLetter = new DeadLetter(record.Message.Key, record.Message.Value ?? "",
            reason, DateTimeOffset.UtcNow);
        await producer.ProduceAsync(kafka.Value.DeadLetterTopic, new Message<string, string>
        {
            Key = record.Message.Key,
            Value = JsonSerializer.Serialize(deadLetter, WireJson.Options)
        }, ct);
        Telemetry.DeadLetters.Add(1);
        logger.LogError("Message at {Offset} sent to DLQ: {Reason}", record.TopicPartitionOffset, reason);
    }
}
