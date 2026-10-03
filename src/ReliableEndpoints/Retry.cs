namespace ReliableEndpoints;

public sealed record RetryOptions
{
    public int MaximumAttempts { get; init; } = 3;
    public TimeSpan Delay { get; init; } = TimeSpan.FromMilliseconds(100);
    public required Func<Exception, bool> ShouldRetry { get; init; }
}

public static class Retry
{
    /// <summary>Retries an explicitly safe operation, never the entire HTTP pipeline.</summary>
    public static async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, RetryOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(options);
        if (options.MaximumAttempts < 1 || options.Delay < TimeSpan.Zero || options.ShouldRetry is null)
            throw new ArgumentException("Invalid retry options.", nameof(options));
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try { return await operation(cancellationToken); }
            catch (Exception exception) when (exception is not OperationCanceledException && attempt < options.MaximumAttempts && options.ShouldRetry(exception))
            { await Task.Delay(options.Delay, cancellationToken); }
        }
    }
}
