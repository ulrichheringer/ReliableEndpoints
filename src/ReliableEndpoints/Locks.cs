namespace ReliableEndpoints;

/// <summary>A shared backend must guarantee exclusive ownership until the returned handle is disposed.
/// Return null immediately on contention. Backend failures must throw, never grant a lock.</summary>
public interface IDistributedLockProvider
{
    ValueTask<IAsyncDisposable?> TryAcquireAsync(string key, CancellationToken cancellationToken);
}

/// <summary>Process-local implementation; replace for multiple application instances.</summary>
public sealed class InMemoryLockProvider : IDistributedLockProvider
{
    private readonly HashSet<string> keys = [];
    private readonly object gate = new();
    public ValueTask<IAsyncDisposable?> TryAcquireAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        cancellationToken.ThrowIfCancellationRequested();
        lock (gate)
            return ValueTask.FromResult<IAsyncDisposable?>(keys.Add(key) ? new Handle(this, key) : null);
    }

    private sealed class Handle(InMemoryLockProvider owner, string key) : IAsyncDisposable
    {
        private int disposed;
        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
                lock (owner.gate) owner.keys.Remove(key);
            return ValueTask.CompletedTask;
        }
    }
}
