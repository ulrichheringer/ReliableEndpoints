using Npgsql;
using Xunit;

namespace ReliableEndpoints.Tests;

public sealed class PostgresTests
{
    [PostgresFact]
    [Trait("Category", "Postgres")]
    public async Task IndependentInstancesExcludeConcurrentExecutionAndReplayDurably()
    {
        var connectionString = Environment.GetEnvironmentVariable("RELIABLE_ENDPOINTS_POSTGRES");
        await using var source = NpgsqlDataSource.Create(connectionString!);
        await using var first = new PostgresIdempotencyStore(source);
        await using var second = new PostgresIdempotencyStore(source);
        await first.EnsureSchemaAsync();
        var key = Guid.NewGuid().ToString();
        var acquired = await first.AcquireAsync(key, "fp", default);
        Assert.Equal(AcquisitionStatus.Acquired, acquired.Status);
        Assert.Equal(AcquisitionStatus.Busy, (await second.AcquireAsync(key, "fp", default)).Status);
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.CompleteAsync(key, acquired.Lease!, new(200, new Dictionary<string, string[]>(), []), TimeSpan.FromMinutes(1), default).AsTask());
        await first.CompleteAsync(key, acquired.Lease!, new(201, new Dictionary<string, string[]> { ["Content-Type"] = ["application/json"] }, [1, 2]), TimeSpan.FromMinutes(1), default);
        await first.ReleaseAsync(key, acquired.Lease!, default);
        var replay = await second.AcquireAsync(key, "fp", default);
        Assert.Equal(AcquisitionStatus.Replay, replay.Status);
        Assert.Equal(201, replay.Response!.StatusCode);
        Assert.Equal(new byte[] { 1, 2 }, replay.Response.Body);
        Assert.Equal(AcquisitionStatus.Conflict, (await second.AcquireAsync(key, "other", default)).Status);
        var lockOne = new PostgresLockProvider(source);
        var lockTwo = new PostgresLockProvider(source);
        var held = await lockOne.TryAcquireAsync(key, default);
        Assert.NotNull(held);
        Assert.Null(await lockTwo.TryAcquireAsync(key, default));
        await held!.DisposeAsync();
        var next = await lockTwo.TryAcquireAsync(key, default);
        Assert.NotNull(next);
        await next!.DisposeAsync();
    }
}

public sealed class PostgresFactAttribute : FactAttribute
{
    public PostgresFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RELIABLE_ENDPOINTS_POSTGRES")))
            Skip = "Set RELIABLE_ENDPOINTS_POSTGRES to run real PostgreSQL integration tests.";
    }
}
