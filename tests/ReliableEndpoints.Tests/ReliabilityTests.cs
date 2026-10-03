using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using ReliableEndpoints;
using Xunit;

namespace ReliableEndpoints.Tests;

public sealed class ReliabilityTests
{
    private static TestServer Server(RequestDelegate endpoint, TimeSpan? timeout = null, int limit = 1024, string? lockKey = null)
    {
        return new TestServer(new WebHostBuilder().ConfigureServices(services => { services.AddRouting(); services.AddReliableEndpoints(); })
            .Configure(app =>
            {
                app.UseRouting();
                app.UseReliableEndpoints();
                app.UseEndpoints(routes => routes.MapPost("/orders", endpoint).WithReliability(new EndpointReliabilityOptions
                {
                    KeyScope = context => context.Request.Headers["Tenant"].FirstOrDefault() ?? "test",
                    Timeout = timeout ?? TimeSpan.FromSeconds(10),
                    MaximumBodyBytes = limit,
                    LockKey = lockKey is null ? null : _ => lockKey
                }));
            }));
    }

    private static HttpRequestMessage Request(string? key = "one", string body = "body", string tenant = "test")
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/orders") { Content = new StringContent(body) };
        if (key is not null) request.Headers.Add("Idempotency-Key", key);
        request.Headers.Add("Tenant", tenant);
        return request;
    }

    [Fact]
    public async Task ReplaysBodyStatusAndSafeHeadersWithoutExecutingAgain()
    {
        var calls = 0;
        using var server = Server(async context =>
        {
            calls++;
            context.Response.StatusCode = 201;
            context.Response.ContentType = "application/json";
            context.Response.ContentLength = 10;
            context.Response.Headers.Location = "/orders/123";
            context.Response.Headers.SetCookie = "secret=value";
            await context.Response.WriteAsync("{\"id\":123}");
        });
        using var client = server.CreateClient();
        using var first = await client.SendAsync(Request());
        using var replay = await client.SendAsync(Request());
        Assert.Equal(HttpStatusCode.Created, replay.StatusCode);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
        Assert.Equal("/orders/123", replay.Headers.Location?.ToString());
        Assert.False(replay.Headers.Contains("Set-Cookie"));
        Assert.True(replay.Headers.Contains("Idempotency-Replayed"));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ChangedPayloadConflictsAndTenantsAreIsolated()
    {
        var calls = 0;
        using var server = Server(context => { calls++; return context.Response.WriteAsync("ok"); });
        using var client = server.CreateClient();
        using var first = await client.SendAsync(Request());
        using var conflict = await client.SendAsync(Request(body: "changed"));
        using var otherTenant = await client.SendAsync(Request(tenant: "other"));
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        Assert.Equal(HttpStatusCode.OK, otherTenant.StatusCode);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task ChangedQueryConflicts()
    {
        using var server = Server(context => context.Response.WriteAsync("ok"));
        using var client = server.CreateClient();
        using var request = Request();
        request.RequestUri = new Uri("/orders?a=1", UriKind.Relative);
        using var first = await client.SendAsync(request);
        using var changed = Request();
        changed.RequestUri = new Uri("/orders?a=2", UriKind.Relative);
        using var conflict = await client.SendAsync(changed);
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
    }

    [Fact]
    public async Task RejectsConcurrentSameKey()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var server = Server(async context => { entered.SetResult(); await release.Task; await context.Response.WriteAsync("ok"); });
        using var client = server.CreateClient();
        var first = client.SendAsync(Request());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        using var busy = await client.SendAsync(Request());
        Assert.Equal(HttpStatusCode.Conflict, busy.StatusCode);
        release.SetResult();
        using var result = await first;
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    [Fact]
    public async Task SharedLockSerializesDifferentIdempotencyKeys()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var server = Server(async context => { entered.SetResult(); await release.Task; }, lockKey: "resource");
        using var client = server.CreateClient();
        var first = client.SendAsync(Request("one"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        using var busy = await client.SendAsync(Request("two"));
        Assert.Equal(HttpStatusCode.Conflict, busy.StatusCode);
        release.SetResult();
        using var result = await first;
    }

    [Fact]
    public async Task CooperativeTimeoutReturns504AndReleasesLease()
    {
        var calls = 0;
        using var server = Server(async context =>
        {
            if (Interlocked.Increment(ref calls) == 1) await Task.Delay(1000, context.RequestAborted);
            await context.Response.WriteAsync("ok");
        }, TimeSpan.FromMilliseconds(50));
        using var client = server.CreateClient();
        using var first = await client.SendAsync(Request());
        using var second = await client.SendAsync(Request());
        Assert.Equal(HttpStatusCode.GatewayTimeout, first.StatusCode);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
    }

    [Fact]
    public async Task ServerErrorsAreNotStored()
    {
        var calls = 0;
        using var server = Server(context => { context.Response.StatusCode = ++calls == 1 ? 503 : 200; return Task.CompletedTask; });
        using var client = server.CreateClient();
        using var first = await client.SendAsync(Request());
        using var second = await client.SendAsync(Request());
        Assert.Equal(HttpStatusCode.ServiceUnavailable, first.StatusCode);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task ValidatesMissingKeyAndRequestSizeBeforeExecution()
    {
        var calls = 0;
        using var server = Server(context => { calls++; return Task.CompletedTask; }, limit: 2);
        using var client = server.CreateClient();
        using var missing = await client.SendAsync(Request(null, ""));
        using var large = await client.SendAsync(Request());
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, large.StatusCode);
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task StoreEnforcesFingerprintAndFencesLeases()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.AcquireAsync("key", "fingerprint", default);
        Assert.Equal(AcquisitionStatus.Busy, (await store.AcquireAsync("key", "fingerprint", default)).Status);
        Assert.Equal(AcquisitionStatus.Conflict, (await store.AcquireAsync("key", "changed", default)).Status);
        await store.ReleaseAsync("key", "wrong", default);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.CompleteAsync("key", "wrong", new(200, new Dictionary<string, string[]>(), []), TimeSpan.FromMinutes(1), default).AsTask());
        await store.ReleaseAsync("key", first.Lease!, default);
        Assert.Equal(AcquisitionStatus.Acquired, (await store.AcquireAsync("key", "changed", default)).Status);
    }

    [Fact]
    public async Task StoreExpirationUsesInjectedClockAndCopiesPayload()
    {
        var clock = new ManualClock();
        var store = new InMemoryIdempotencyStore(clock);
        var first = await store.AcquireAsync("key", "fp", default);
        byte[] body = [1];
        await store.CompleteAsync("key", first.Lease!, new(200, new Dictionary<string, string[]>(), body), TimeSpan.FromSeconds(1), default);
        body[0] = 2;
        var replay = await store.AcquireAsync("key", "fp", default);
        Assert.Equal(1, replay.Response!.Body[0]);
        replay.Response.Body[0] = 3;
        Assert.Equal(1, (await store.AcquireAsync("key", "fp", default)).Response!.Body[0]);
        clock.Now += TimeSpan.FromSeconds(2);
        Assert.Equal(AcquisitionStatus.Acquired, (await store.AcquireAsync("key", "new", default)).Status);
    }

    [Fact]
    public async Task RetryIsBoundedAndCancellationIsNeverRetried()
    {
        var attempts = 0;
        var value = await Retry.ExecuteAsync(_ => ++attempts < 3 ? Task.FromException<int>(new IOException()) : Task.FromResult(42),
            new RetryOptions { MaximumAttempts = 3, Delay = TimeSpan.Zero, ShouldRetry = exception => exception is IOException });
        Assert.Equal(42, value);
        Assert.Equal(3, attempts);
        attempts = 0;
        await Assert.ThrowsAsync<OperationCanceledException>(() => Retry.ExecuteAsync<int>(_ => { attempts++; throw new OperationCanceledException(); },
            new RetryOptions { Delay = TimeSpan.Zero, ShouldRetry = _ => true }));
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task LockDisposeIsIdempotent()
    {
        var locks = new InMemoryLockProvider();
        var held = await locks.TryAcquireAsync("key", default);
        Assert.Null(await locks.TryAcquireAsync("key", default));
        await held!.DisposeAsync();
        var next = await locks.TryAcquireAsync("key", default);
        await held.DisposeAsync();
        Assert.Null(await locks.TryAcquireAsync("key", default));
        await next!.DisposeAsync();
    }

    private sealed class ManualClock : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => Now;
    }
}
