using System.Globalization;
using ImportForge.Infrastructure.Db;

namespace ImportForge.Infrastructure.Repositories;

public sealed class ImportRowsRepository
{
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
}
