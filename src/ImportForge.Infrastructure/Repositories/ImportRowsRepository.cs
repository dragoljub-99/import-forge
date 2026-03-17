using System.Globalization;
using ImportForge.Infrastructure.Db;

namespace ImportForge.Infrastructure.Repositories;

public sealed class ImportRowsRepository
{
    private const string UnknownFieldName = "unknown";

    private readonly DbConnectionFactory _connectionFactory;

    public ImportRowsRepository(DbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<long> AddAsync(ImportRowForInsert row, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO ImportRows (JobId, RowNumber, ProductId, ProductName, ProductRsdValue, ProductQuantity)
            VALUES (@jobId, @rowNumber, @productId, @productName, @productRsdValue, @productQuantity);
            SELECT last_insert_rowid();
            """;
        command.Parameters.AddWithValue("@jobId", row.JobId);
        command.Parameters.AddWithValue("@rowNumber", row.RowNumber);
        command.Parameters.AddWithValue("@productId", (object?)row.ProductId ?? DBNull.Value);
        command.Parameters.AddWithValue("@productName", (object?)row.ProductName ?? DBNull.Value);
        command.Parameters.AddWithValue("@productRsdValue", (object?)row.ProductRsdValue ?? DBNull.Value);
        command.Parameters.AddWithValue("@productQuantity", (object?)row.ProductQuantity ?? DBNull.Value);

        var scalar = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
    }

    public async Task<IReadOnlyList<ImportRowForValidation>> ListByJobIdAsync(long jobId, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT Id, JobId, RowNumber, ProductId, ProductName, ProductRsdValue, ProductQuantity
            FROM ImportRows
            WHERE JobId = @jobId
            ORDER BY RowNumber ASC;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);

        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<ImportRowForValidation>();

        while (await reader.ReadAsync(ct))
        {
            var productIdOrdinal = reader.GetOrdinal("ProductId");
            var productNameOrdinal = reader.GetOrdinal("ProductName");
            var productRsdValueOrdinal = reader.GetOrdinal("ProductRsdValue");
            var productQuantityOrdinal = reader.GetOrdinal("ProductQuantity");

            rows.Add(
                new ImportRowForValidation(
                    reader.GetInt64(reader.GetOrdinal("Id")),
                    reader.GetInt64(reader.GetOrdinal("JobId")),
                    reader.GetInt32(reader.GetOrdinal("RowNumber")),
                    reader.IsDBNull(productIdOrdinal) ? null : reader.GetString(productIdOrdinal),
                    reader.IsDBNull(productNameOrdinal) ? null : reader.GetString(productNameOrdinal),
                    reader.IsDBNull(productRsdValueOrdinal) ? null : reader.GetInt32(productRsdValueOrdinal),
                    reader.IsDBNull(productQuantityOrdinal) ? null : reader.GetInt32(productQuantityOrdinal)));
        }

        return rows;
    }

    public async Task<IReadOnlyList<ImportRowForValidation>> ListValidatableByJobIdAsync(long jobId, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT r.Id, r.JobId, r.RowNumber, r.ProductId, r.ProductName, r.ProductRsdValue, r.ProductQuantity
            FROM ImportRows r
            WHERE r.JobId = @jobId
                AND NOT EXISTS (
                    SELECT 1
                    FROM ImportRowErrors e
                    WHERE e.RowId = r.Id
                        AND e.Field = @unknownField
                )
            ORDER BY r.RowNumber ASC;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);
        command.Parameters.AddWithValue("@unknownField", UnknownFieldName);

        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<ImportRowForValidation>();

        while (await reader.ReadAsync(ct))
        {
            var productIdOrdinal = reader.GetOrdinal("ProductId");
            var productNameOrdinal = reader.GetOrdinal("ProductName");
            var productRsdValueOrdinal = reader.GetOrdinal("ProductRsdValue");
            var productQuantityOrdinal = reader.GetOrdinal("ProductQuantity");

            rows.Add(
                new ImportRowForValidation(
                    reader.GetInt64(reader.GetOrdinal("Id")),
                    reader.GetInt64(reader.GetOrdinal("JobId")),
                    reader.GetInt32(reader.GetOrdinal("RowNumber")),
                    reader.IsDBNull(productIdOrdinal) ? null : reader.GetString(productIdOrdinal),
                    reader.IsDBNull(productNameOrdinal) ? null : reader.GetString(productNameOrdinal),
                    reader.IsDBNull(productRsdValueOrdinal) ? null : reader.GetInt32(productRsdValueOrdinal),
                    reader.IsDBNull(productQuantityOrdinal) ? null : reader.GetInt32(productQuantityOrdinal)));
        }

        return rows;
    }
}
