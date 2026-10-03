using System.Collections.Concurrent;

namespace ReliableEndpoints;

public enum AcquisitionStatus { Acquired, Busy, Replay, Conflict }

public sealed record StoredResponse(int StatusCode, IReadOnlyDictionary<string, string[]> Headers, byte[] Body);

public sealed record Acquisition(AcquisitionStatus Status, string? Lease = null, StoredResponse? Response = null);

/// <summary>Implement atomically in a shared backend for multi-instance deployments.
/// Active leases must remain exclusive until released; implementations must fence completion by lease token.</summary>
public interface IIdempotencyStore
{
    ValueTask<Acquisition> AcquireAsync(string key, string fingerprint, CancellationToken cancellationToken);
    ValueTask CompleteAsync(string key, string lease, StoredResponse response, TimeSpan retention, CancellationToken cancellationToken);
    ValueTask ReleaseAsync(string key, string lease, CancellationToken cancellationToken);
}

/// <summary>Process-local reference implementation. Active leases do not expire.</summary>
public sealed class InMemoryIdempotencyStore(TimeProvider? clock = null) : IIdempotencyStore
{
    private sealed record Entry(string Fingerprint, string Lease, StoredResponse? Response, DateTimeOffset Expires);
    private readonly ConcurrentDictionary<string, Entry> entries = new();
    private readonly object gate = new();
    private readonly TimeProvider clock = clock ?? TimeProvider.System;

    public ValueTask<Acquisition> AcquireAsync(string key, string fingerprint, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (gate)
        {
            if (entries.TryGetValue(key, out var entry))
            {
                if (entry.Response is not null && entry.Expires <= clock.GetUtcNow())
                    entries.TryRemove(key, out _);
                else
                    return ValueTask.FromResult(entry.Fingerprint != fingerprint
                        ? new Acquisition(AcquisitionStatus.Conflict)
                        : entry.Response is null
                            ? new Acquisition(AcquisitionStatus.Busy)
                            : new Acquisition(AcquisitionStatus.Replay, Response: Clone(entry.Response)));
            }
            var lease = Guid.NewGuid().ToString("N");
            entries[key] = new Entry(fingerprint, lease, null, DateTimeOffset.MaxValue);
            return ValueTask.FromResult(new Acquisition(AcquisitionStatus.Acquired, lease));
        }
    }

    public ValueTask CompleteAsync(string key, string lease, StoredResponse response, TimeSpan retention, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (gate)
        {
            if (!entries.TryGetValue(key, out var entry) || entry.Lease != lease || entry.Response is not null)
                throw new InvalidOperationException("Lease is no longer active.");
            entries[key] = entry with { Response = Clone(response), Expires = clock.GetUtcNow() + retention };
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask ReleaseAsync(string key, string lease, CancellationToken cancellationToken)
    {
        lock (gate)
            if (entries.TryGetValue(key, out var entry) && entry.Lease == lease && entry.Response is null)
                entries.TryRemove(key, out _);
        return ValueTask.CompletedTask;
    }

    private static StoredResponse Clone(StoredResponse response) => response with
    {
        Body = response.Body.ToArray(),
        Headers = response.Headers.ToDictionary(pair => pair.Key, pair => pair.Value.ToArray(), StringComparer.OrdinalIgnoreCase)
    };
}
