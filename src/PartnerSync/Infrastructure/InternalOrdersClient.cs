using Grpc.Core;
using Microsoft.Extensions.Options;
using PartnerSync.Contracts;
using PartnerSync.Contracts.Grpc;

namespace PartnerSync.Infrastructure;

public sealed class InternalOrdersClient(
    InternalOrders.InternalOrdersClient client, IOptions<SyncSettings> settings)
{
    private Metadata Headers => new() { { "x-api-key", settings.Value.InternalApiKey } };
    private DateTime Deadline => DateTime.UtcNow.AddSeconds(settings.Value.RequestTimeoutSeconds);

    public async Task<OrderSnapshot?> GetAsync(string partnerId, string orderId, CancellationToken ct)
    {
        var reply = await client.GetOrderAsync(new GetOrderRequest
        {
            PartnerId = partnerId, OrderId = orderId
        }, Headers, Deadline, ct);
        return reply.Found ? reply.Order : null;
    }

    public async Task<ApplyOutcome> ApplyAsync(RepairRequested message, CancellationToken ct)
    {
        var reply = await client.ApplyPartnerOrderAsync(new ApplyPartnerOrderRequest
        {
            EventId = message.EventId.ToString(),
            ExpectedRevision = message.ExpectedRevision,
            Order = new OrderSnapshot
            {
                PartnerId = message.PartnerId,
                OrderId = message.Order.OrderId,
                Status = message.Order.Status,
                TotalMinor = message.Order.TotalMinor,
                Currency = message.Order.Currency,
                SourceVersion = message.Order.Version
            }
        }, Headers, Deadline, ct);
        if ((int)reply.Outcome is < 1 or > 3)
            throw new InvalidDataException("Internal system returned an unknown apply outcome.");
        return reply.Outcome;
    }
}
