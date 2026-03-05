using System.Globalization;
using ImportForge.Domain;
using ImportForge.Infrastructure.Db;

namespace ImportForge.Infrastructure.Repositories;

public sealed class ImportJobsRepository
{
    private readonly DbConnectionFactory _connectionFactory;

    public ImportJobsRepository(DbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<long> CreateAsync(ImportJobStatus status, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO ImportJobs (Status)
            VALUES (@status);
            SELECT last_insert_rowid();
            """;
        command.Parameters.AddWithValue("@status", ImportJobStatusDbTokens.ToToken(status));

        var scalar = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<ImportJob?> GetByIdAsync(long jobId, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT Id, Status, TotalRows, ValidRows, InvalidRows, ClearedAt
            FROM ImportJobs
            WHERE Id = @jobId
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var statusToken = reader.GetString(reader.GetOrdinal("Status"));
        var clearedAtOrdinal = reader.GetOrdinal("ClearedAt");

        DateTimeOffset? clearedAt = null;
        if (!reader.IsDBNull(clearedAtOrdinal))
        {
            var clearedAtText = reader.GetString(clearedAtOrdinal);
            clearedAt = DateTimeOffset.Parse(clearedAtText, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        return new ImportJob(
            reader.GetInt64(reader.GetOrdinal("Id")),
            ImportJobStatusDbTokens.FromToken(statusToken),
            reader.GetInt32(reader.GetOrdinal("TotalRows")),
            reader.GetInt32(reader.GetOrdinal("ValidRows")),
            reader.GetInt32(reader.GetOrdinal("InvalidRows")),
            clearedAt);
    }

    public async Task UpdateStatusAsync(long jobId, ImportJobStatus status, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE ImportJobs
            SET Status = @status
            WHERE Id = @jobId;
            """;
        command.Parameters.AddWithValue("@status", ImportJobStatusDbTokens.ToToken(status));
        command.Parameters.AddWithValue("@jobId", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task UpdateCountersAsync(long jobId, int totalRows, int validRows, int invalidRows, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE ImportJobs
            SET
                TotalRows = @totalRows,
                ValidRows = @validRows,
                InvalidRows = @invalidRows
            WHERE Id = @jobId;
            """;
        command.Parameters.AddWithValue("@totalRows", totalRows);
        command.Parameters.AddWithValue("@validRows", validRows);
        command.Parameters.AddWithValue("@invalidRows", invalidRows);
        command.Parameters.AddWithValue("@jobId", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task SetClearedAtAsync(long jobId, DateTimeOffset clearedAtUtc, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE ImportJobs
            SET ClearedAt = @clearedAt
            WHERE Id = @jobId;
            """;
        command.Parameters.AddWithValue("@clearedAt", clearedAtUtc.ToString("O", CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue("@jobId", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
