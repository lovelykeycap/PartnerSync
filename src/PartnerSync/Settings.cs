namespace PartnerSync;

public sealed class SyncSettings
{
    public int PollIntervalSeconds { get; set; } = 30;
    public int RequestTimeoutSeconds { get; set; } = 10;
    public int MaxPagesPerSweep { get; set; } = 1000;
    public int MaxApplyAttempts { get; set; } = 5;
    public string InternalGrpcUrl { get; set; } = "http://localhost:8082";
    public string InternalApiKey { get; set; } = "";
    public List<PartnerSettings> Partners { get; set; } = [];

    public bool IsValid() => PollIntervalSeconds > 0 && RequestTimeoutSeconds is > 0 and <= 60 &&
        MaxPagesPerSweep > 0 && MaxApplyAttempts is > 0 and <= 10 &&
        MaxApplyAttempts * RequestTimeoutSeconds + (MaxApplyAttempts - 1) * 11 <= 240 &&
        Uri.TryCreate(InternalGrpcUrl, UriKind.Absolute, out var uri) &&
        uri.Scheme is "http" or "https" && !string.IsNullOrWhiteSpace(InternalApiKey) &&
        Partners.Count > 0 && Partners.Select(p => p.Id).Distinct().Count() == Partners.Count &&
        Partners.All(p => p.IsValid());
}

public sealed class PartnerSettings
{
    public string Id { get; set; } = "";
    public string BaseUrl { get; set; } = "";
    public string ApiKey { get; set; } = "";
    public int RequestsPerSecond { get; set; } = 5;

    public bool IsValid() => Id.Length is > 0 and <= 60 &&
        Id.All(c => char.IsAsciiLetterOrDigit(c) || c == '-') &&
        Uri.TryCreate(BaseUrl, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" &&
        !string.IsNullOrWhiteSpace(ApiKey) && RequestsPerSecond is > 0 and <= 1000;
}

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string RepairTopic { get; set; } = "partner-sync.repairs.v1";
    public string DeadLetterTopic { get; set; } = "partner-sync.repairs.dlq.v1";
    public string GroupId { get; set; } = "partner-sync-apply-v1";
}
