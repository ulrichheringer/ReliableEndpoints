using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace ReliableEndpoints;

/// <summary>Session advisory locks: one dedicated pooled connection per active lock, released on disposal.</summary>
public sealed class PostgresLockProvider(NpgsqlDataSource dataSource) : IDistributedLockProvider
{
    public async ValueTask<IAsyncDisposable?> TryAcquireAsync(string key, CancellationToken cancellationToken)
        => await AcquireConnectionAsync(dataSource, "lock:" + key, cancellationToken);

    internal static async Task<SessionLock?> AcquireConnectionAsync(NpgsqlDataSource source, string key, CancellationToken cancellationToken)
    {
        var connection = await source.OpenConnectionAsync(cancellationToken);
        var id = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
        try
        {
            await using var command = new NpgsqlCommand("SELECT pg_try_advisory_lock($1)", connection);
            command.Parameters.AddWithValue(id);
            if ((bool)(await command.ExecuteScalarAsync(cancellationToken))!) return new SessionLock(connection, id);
            await connection.DisposeAsync();
            return null;
        }
        catch { await connection.DisposeAsync(); throw; }
    }

    internal sealed class SessionLock(NpgsqlConnection connection, long id) : IAsyncDisposable
    {
        public NpgsqlConnection Connection { get; } = connection;
        private int disposed;
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0) return;
            try
            {
                await using var command = new NpgsqlCommand("SELECT pg_advisory_unlock($1)", Connection);
                command.Parameters.AddWithValue(id);
                await command.ExecuteNonQueryAsync();
            }
            finally { await Connection.DisposeAsync(); }
        }
    }
}

/// <summary>Durable response storage plus PostgreSQL session advisory locks across application instances.</summary>
public sealed class PostgresIdempotencyStore(NpgsqlDataSource dataSource) : IIdempotencyStore, IAsyncDisposable
{
    private sealed record Lease(string Key, string Fingerprint, PostgresLockProvider.SessionLock Lock);
    private readonly ConcurrentDictionary<string, Lease> leases = new();

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            CREATE TABLE IF NOT EXISTS reliable_endpoint_responses (
              key text PRIMARY KEY, fingerprint text NOT NULL, response jsonb NOT NULL,
              expires_at timestamptz NOT NULL
            )
            """);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask<Acquisition> AcquireAsync(string key, string fingerprint, CancellationToken cancellationToken)
    {
        var held = await PostgresLockProvider.AcquireConnectionAsync(dataSource, "idempotency:" + key, cancellationToken);
        if (held is null) return new Acquisition(AcquisitionStatus.Busy);
        try
        {
            await using var command = new NpgsqlCommand("SELECT fingerprint, response::text FROM reliable_endpoint_responses WHERE key = $1 AND expires_at > clock_timestamp()", held.Connection);
            command.Parameters.AddWithValue(key);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var result = reader.GetString(0) == fingerprint
                    ? new Acquisition(AcquisitionStatus.Replay, Response: JsonSerializer.Deserialize<StoredResponse>(reader.GetString(1))!)
                    : new Acquisition(AcquisitionStatus.Conflict);
                await reader.DisposeAsync();
                await held.DisposeAsync();
                return result;
            }
            var token = Guid.NewGuid().ToString("N");
            leases[token] = new Lease(key, fingerprint, held);
            return new Acquisition(AcquisitionStatus.Acquired, token);
        }
        catch { await held.DisposeAsync(); throw; }
    }

    public async ValueTask CompleteAsync(string key, string lease, StoredResponse response, TimeSpan retention, CancellationToken cancellationToken)
    {
        if (!leases.TryGetValue(lease, out var owner) || owner.Key != key)
            throw new InvalidOperationException("Lease is no longer active.");
        await using var command = new NpgsqlCommand("""
            INSERT INTO reliable_endpoint_responses (key, fingerprint, response, expires_at)
            VALUES ($1, $2, $3::jsonb, clock_timestamp() + $4)
            ON CONFLICT (key) DO UPDATE SET fingerprint = EXCLUDED.fingerprint,
              response = EXCLUDED.response, expires_at = EXCLUDED.expires_at
            """, owner.Lock.Connection);
        command.Parameters.AddWithValue(key);
        command.Parameters.AddWithValue(owner.Fingerprint);
        command.Parameters.AddWithValue(JsonSerializer.Serialize(response));
        command.Parameters.AddWithValue(retention);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask ReleaseAsync(string key, string lease, CancellationToken cancellationToken)
    {
        if (leases.TryGetValue(lease, out var owner) && owner.Key == key && leases.TryRemove(lease, out owner))
            await owner.Lock.DisposeAsync();
    }

    /// <summary>Run periodically to bound retained storage. Does not affect active session locks.</summary>
    public async Task<int> PurgeExpiredAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM reliable_endpoint_responses WHERE expires_at <= clock_timestamp()");
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var lease in leases.Keys)
            if (leases.TryRemove(lease, out var owner)) await owner.Lock.DisposeAsync();
    }
}
