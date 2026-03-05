using ImportForge.Infrastructure.Db;

namespace ImportForge.Infrastructure.Repositories;

public sealed class ImportRowErrorsRepository
{
    private readonly DbConnectionFactory _connectionFactory;

    public ImportRowErrorsRepository(DbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task AddAsync(long rowId, string field, string error, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT OR IGNORE INTO ImportRowErrors (RowId, Field, Error)
            VALUES (@rowId, @field, @error);
            """;
        command.Parameters.AddWithValue("@rowId", rowId);
        command.Parameters.AddWithValue("@field", field);
        command.Parameters.AddWithValue("@error", error);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<ImportJobErrorItem>> ListByJobIdAsync(long jobId, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT r.RowNumber, e.Field, e.Error
            FROM ImportRowErrors e
            INNER JOIN ImportRows r ON r.Id = e.RowId
            WHERE r.JobId = @jobId
            ORDER BY r.RowNumber ASC, e.Id ASC;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);

        await using var reader = await command.ExecuteReaderAsync(ct);
        var items = new List<ImportJobErrorItem>();

        while (await reader.ReadAsync(ct))
        {
            items.Add(
                new ImportJobErrorItem(
                    reader.GetInt32(reader.GetOrdinal("RowNumber")),
                    reader.GetString(reader.GetOrdinal("Field")),
                    reader.GetString(reader.GetOrdinal("Error"))));
        }

        return items;
    }
}
