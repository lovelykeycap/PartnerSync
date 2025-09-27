using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Options;
using PartnerSync.Contracts;
using StackExchange.Redis;

namespace PartnerSync.Infrastructure;

public sealed class PartnerClient(
    IHttpClientFactory clients,
    IConnectionMultiplexer redis,
    IOptions<SyncSettings> settings,
    ILogger<PartnerClient> logger)
{
    // Redis time keeps replicas in the same window. Only rate limiting depends on Redis.
    private const string RateLimitScript = """
        local second = redis.call('TIME')[1]
        local key = KEYS[1] .. ':' .. second
        local count = redis.call('INCR', key)
        if count == 1 then redis.call('EXPIRE', key, 2) end
        return count <= tonumber(ARGV[1]) and 1 or 0
        """;

    public async Task<OrderPage> ReadPageAsync(PartnerSettings partner, string? cursor,
        CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            // Includes waiting for the shared quota and reading the response body.
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(settings.Value.RequestTimeoutSeconds));
            try
            {
                await WaitForQuotaAsync(partner, timeout.Token);
                var relative = "orders?limit=100" +
                    (cursor is null ? "" : $"&cursor={Uri.EscapeDataString(cursor)}");
                using var request = new HttpRequestMessage(HttpMethod.Get,
                    new Uri(new Uri(partner.BaseUrl), relative));
                request.Headers.Add("X-Api-Key", partner.ApiKey);
                using var response = await clients.CreateClient("partners")
                    .SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token);

                if ((response.StatusCode == HttpStatusCode.TooManyRequests ||
                     (int)response.StatusCode >= 500) && attempt < 3)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta ??
                        (response.Headers.RetryAfter?.Date - DateTimeOffset.UtcNow);
                    await Task.Delay(retryAfter is { } delay
                        ? TimeSpan.FromMilliseconds(Math.Clamp(delay.TotalMilliseconds, 100, 30_000))
                        : Backoff(attempt), ct);
                    continue;
                }

                response.EnsureSuccessStatusCode();
                var page = await response.Content.ReadFromJsonAsync<OrderPage>(WireJson.Options, timeout.Token)
                    ?? throw new InvalidDataException("Partner returned an empty response.");
                if (page.Items is null || page.Items.Count > 100 ||
                    page.Items.Any(o => !OrderRules.IsValid(o)))
                    throw new InvalidDataException("Partner returned invalid orders.");
                return page;
            }
            catch (Exception ex) when (attempt < 3 && !ct.IsCancellationRequested &&
                (ex is HttpRequestException { StatusCode: null } || ex is OperationCanceledException))
            {
                logger.LogWarning("Partner {PartnerId} page request timed out or lost connection; retry {Attempt}",
                    partner.Id, attempt + 1);
                await Task.Delay(Backoff(attempt), ct);
            }
        }
    }

    private async Task WaitForQuotaAsync(PartnerSettings partner, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var permitted = (long)await redis.GetDatabase().ScriptEvaluateAsync(RateLimitScript,
                [(RedisKey)$"partner-sync:rate:{partner.Id}"], [(RedisValue)partner.RequestsPerSecond])
                .WaitAsync(ct);
            if (permitted == 1) return;
            await Task.Delay(TimeSpan.FromMilliseconds(100 + Random.Shared.Next(50)), ct);
        }
    }

    private static TimeSpan Backoff(int attempt) =>
        TimeSpan.FromMilliseconds(250 * Math.Pow(2, attempt) + Random.Shared.Next(150));
}
