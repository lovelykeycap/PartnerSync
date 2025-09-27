using Microsoft.Extensions.Options;
using Npgsql;
using PartnerSync.Infrastructure;

namespace PartnerSync.Workers;

public sealed class PollingWorker(
    NpgsqlDataSource database,
    PartnerClient partners,
    InternalOrdersClient internalOrders,
    ReconciliationStore store,
    IOptions<SyncSettings> settings,
    ILogger<PollingWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.WhenAll(settings.Value.Partners.Select(p => RunPartnerAsync(p, stoppingToken)));
    }

    private async Task RunPartnerAsync(PartnerSettings partner, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await SweepAsync(partner, ct); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogError(ex, "Sweep failed for partner {PartnerId}", partner.Id); }
            await Task.Delay(TimeSpan.FromSeconds(settings.Value.PollIntervalSeconds), ct);
        }
    }

    private async Task SweepAsync(PartnerSettings partner, CancellationToken ct)
    {
        using var activity = Telemetry.Activities.StartActivity("partner.sweep");
        activity?.SetTag("partner.id", partner.Id);
        // A session lock has no TTL that can expire while an API call is in flight.
        // Always release it before returning this connection to the pool.
        await using var connection = await database.OpenConnectionAsync(ct);
        await using var acquire = new NpgsqlCommand(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", connection);
        acquire.Parameters.AddWithValue($"poll:{partner.Id}");
        if (await acquire.ExecuteScalarAsync(ct) is not true) return;

        try
        {
            string? cursor = null;
            var cursors = new HashSet<string>();
            var count = 0;
            for (var pageNumber = 0; pageNumber < settings.Value.MaxPagesPerSweep; pageNumber++)
            {
                var page = await partners.ReadPageAsync(partner, cursor, ct);
                foreach (var order in page.Items)
                {
                    var current = await internalOrders.GetAsync(partner.Id, order.OrderId, ct);
                    await store.RecordAsync(connection, partner.Id, order, current, ct);
                    count++;
                }

                cursor = page.NextCursor;
                if (cursor is null)
                {
                    Telemetry.Sweeps.Add(1);
                    logger.LogInformation("Partner {PartnerId}: inspected {OrderCount} orders", partner.Id, count);
                    return;
                }
                if (cursor.Length > 2048 || !cursors.Add(cursor))
                    throw new InvalidDataException("Partner returned an invalid or repeated cursor.");
            }
            throw new InvalidDataException("Partner exceeded the page limit.");
        }
        finally
        {
            await using var release = new NpgsqlCommand(
                "SELECT pg_advisory_unlock(hashtextextended($1, 0))", connection);
            release.Parameters.AddWithValue($"poll:{partner.Id}");
            await release.ExecuteScalarAsync(CancellationToken.None);
        }
    }
}
