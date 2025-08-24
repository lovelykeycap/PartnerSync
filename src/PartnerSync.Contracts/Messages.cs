using System.Text.Json;
using System.Text.Json.Serialization;

namespace PartnerSync.Contracts;

public sealed record PartnerOrder(string OrderId, string Status, long TotalMinor,
    string Currency, long Version);

public sealed record OrderPage(List<PartnerOrder> Items, string? NextCursor);

public sealed record RepairRequested(Guid EventId, int SchemaVersion, DateTimeOffset OccurredAt,
    string PartnerId, PartnerOrder Order, long ExpectedRevision);

public sealed record DeadLetter(string? OriginalKey, string OriginalPayload,
    string Reason, DateTimeOffset FailedAt);

public static class WireJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };
}

public static class OrderRules
{
    public static bool IsValid(PartnerOrder? order) =>
        order is not null && !string.IsNullOrWhiteSpace(order.OrderId) && order.OrderId.Length <= 100 &&
        order.Status is "created" or "paid" or "shipped" or "cancelled" &&
        order.TotalMinor >= 0 && order.Version > 0 && order.Currency is { Length: 3 } &&
        order.Currency.All(c => c is >= 'A' and <= 'Z');
}
