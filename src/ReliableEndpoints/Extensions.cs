using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ReliableEndpoints;

public sealed record EndpointReliabilityOptions
{
    public TimeSpan Retention { get; init; } = TimeSpan.FromHours(24);
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);
    public int MaximumBodyBytes { get; init; } = 1024 * 1024;
    public string HeaderName { get; init; } = "Idempotency-Key";
    /// <summary>Required security boundary: return the authenticated tenant or user identifier.</summary>
    public required Func<HttpContext, string> KeyScope { get; init; }
    public Func<HttpContext, string>? LockKey { get; init; }
}

public static class ReliableEndpointExtensions
{
    public static readonly ActivitySource ActivitySource = new("ReliableEndpoints");

    public static IServiceCollection AddReliableEndpoints(this IServiceCollection services)
    {
        services.TryAddSingleton<IIdempotencyStore, InMemoryIdempotencyStore>();
        services.TryAddSingleton<IDistributedLockProvider, InMemoryLockProvider>();
        return services;
    }

    public static T WithReliability<T>(this T builder, EndpointReliabilityOptions options) where T : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Retention <= TimeSpan.Zero || options.Timeout <= TimeSpan.Zero || options.MaximumBodyBytes <= 0
            || string.IsNullOrWhiteSpace(options.HeaderName) || options.KeyScope is null)
            throw new ArgumentException("Retention, timeout and body limit must be positive; header and scope are required.", nameof(options));
        builder.Add(endpoint => endpoint.Metadata.Add(options));
        return builder;
    }

    public static IApplicationBuilder UseReliableEndpoints(this IApplicationBuilder app) => app.Use(async (context, next) =>
    {
        var options = context.GetEndpoint()?.Metadata.GetMetadata<EndpointReliabilityOptions>();
        if (options is null) { await next(context); return; }
        using var activity = ActivitySource.StartActivity("endpoint.execute");
        var header = context.Request.Headers[options.HeaderName];
        if (header.Count != 1 || string.IsNullOrWhiteSpace(header[0]) || header[0]!.Length > 256)
        { context.Response.StatusCode = 400; return; }
        var scope = options.KeyScope(context);
        if (string.IsNullOrWhiteSpace(scope)) throw new InvalidOperationException("KeyScope must return a nonempty identity.");
        context.Request.EnableBuffering();
        using var requestBody = new MemoryStream();
        var chunk = new byte[8192];
        int read;
        while ((read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted)) > 0)
        {
            if (requestBody.Length + read > options.MaximumBodyBytes) { context.Response.StatusCode = 413; return; }
            requestBody.Write(chunk, 0, read);
        }
        context.Request.Body.Position = 0;
        var route = context.Request.Method + " " + context.Request.PathBase + context.Request.Path;
        var key = Hash(Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(new[] { scope, route, header[0]! })));
        var fingerprint = Hash(Encoding.UTF8.GetBytes(context.Request.QueryString + "\n" + context.Request.ContentType + "\n" + Hash(requestBody.ToArray())));
        var store = context.RequestServices.GetRequiredService<IIdempotencyStore>();
        var acquired = await store.AcquireAsync(key, fingerprint, context.RequestAborted);
        activity?.SetTag("idempotency.status", acquired.Status.ToString());
        if (acquired.Status == AcquisitionStatus.Replay)
        { await Replay(context, acquired.Response!); return; }
        if (acquired.Status != AcquisitionStatus.Acquired)
        { context.Response.StatusCode = 409; return; }
        var originalBody = context.Response.Body;
        var originalCancellation = context.RequestAborted;
        await using var buffer = new LimitedBuffer(options.MaximumBodyBytes);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(originalCancellation);
        timeout.CancelAfter(options.Timeout);
        IAsyncDisposable? heldLock = null;
        try
        {
            if (options.LockKey is not null)
            {
                heldLock = await context.RequestServices.GetRequiredService<IDistributedLockProvider>()
                    .TryAcquireAsync(options.LockKey(context), originalCancellation);
                if (heldLock is null) { context.Response.StatusCode = 409; return; }
            }
            context.Response.Body = buffer;
            context.RequestAborted = timeout.Token;
            await next(context);
            timeout.Token.ThrowIfCancellationRequested();
            var headers = context.Response.Headers.Where(pair => pair.Key.Equals("Content-Type", StringComparison.OrdinalIgnoreCase)
                    || pair.Key.Equals("Location", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(pair => pair.Key, pair => pair.Value.Select(value => value ?? "").ToArray());
            var response = new StoredResponse(context.Response.StatusCode, headers, buffer.ToArray());
            if (response.StatusCode < 500)
                await store.CompleteAsync(key, acquired.Lease!, response, options.Retention, CancellationToken.None);
            context.Response.Body = originalBody;
            await originalBody.WriteAsync(response.Body, originalCancellation);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !originalCancellation.IsCancellationRequested)
        {
            context.Response.Clear();
            context.Response.StatusCode = 504;
            activity?.SetStatus(ActivityStatusCode.Error, "timeout");
        }
        finally
        {
            context.Response.Body = originalBody;
            context.RequestAborted = originalCancellation;
            try
            {
                if (heldLock is not null) await heldLock.DisposeAsync();
            }
            finally { await store.ReleaseAsync(key, acquired.Lease!, CancellationToken.None); }
        }
    });

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));
    private static async Task Replay(HttpContext context, StoredResponse response)
    {
        context.Response.StatusCode = response.StatusCode;
        foreach (var header in response.Headers) context.Response.Headers[header.Key] = header.Value;
        context.Response.Headers["Idempotency-Replayed"] = "true";
        await context.Response.Body.WriteAsync(response.Body, context.RequestAborted);
    }

    private sealed class LimitedBuffer(int limit) : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count) { Check(count); base.Write(buffer, offset, count); }
        public override void Write(ReadOnlySpan<byte> buffer) { Check(buffer.Length); base.Write(buffer); }
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        { Check(buffer.Length); return base.WriteAsync(buffer, cancellationToken); }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        { Check(count); return base.WriteAsync(buffer, offset, count, cancellationToken); }
        private void Check(int count)
        { if (Position + count > limit) throw new InvalidOperationException("Response exceeds MaximumBodyBytes."); }
    }
}
