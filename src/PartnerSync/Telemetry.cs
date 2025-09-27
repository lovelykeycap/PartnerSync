using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PartnerSync;

public static class Telemetry
{
    public static readonly ActivitySource Activities = new("PartnerSync");
    private static readonly Meter Meter = new("PartnerSync");
    public static readonly Counter<long> Sweeps = Meter.CreateCounter<long>("partnersync.sweeps");
    public static readonly Counter<long> Inspected = Meter.CreateCounter<long>("partnersync.orders.inspected");
    public static readonly Counter<long> Published = Meter.CreateCounter<long>("partnersync.outbox.published");
    public static readonly Counter<long> Applied = Meter.CreateCounter<long>("partnersync.repairs.applied");
    public static readonly Counter<long> DeadLetters = Meter.CreateCounter<long>("partnersync.repairs.dead_letters");
}
