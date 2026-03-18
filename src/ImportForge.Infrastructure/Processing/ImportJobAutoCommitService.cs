using System.Globalization;
using ImportForge.Domain;
using ImportForge.Infrastructure.Db;
using ImportForge.Infrastructure.Repositories;
using Microsoft.Data.Sqlite;

namespace ImportForge.Infrastructure.Processing;

public sealed class ImportJobAutoCommitService
{
    private const string ProductNameField = "ProductName";
    private const string ProductRsdValueField = "ProductRsdValue";

    private const string ProductNameConflictError = "ProductName conflicts with existing Speakers value for this ProductId.";
    private const string ProductRsdValueConflictError = "ProductRsdValue conflicts with existing Speakers value for this ProductId.";

    private readonly DbConnectionFactory _connectionFactory;

    public ImportJobAutoCommitService(DbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<ImportJobAutoCommitOutcome> ExecuteAsync(long jobId, int totalRows, CancellationToken ct)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(ct);
        var stagedRows = await LoadStagedRowsAsync(connection, jobId, ct);
        var aggregatedByProductId = AggregateByProductId(stagedRows);

        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);
        var existingSpeakers = await LoadExistingSpeakersAsync(connection, transaction, aggregatedByProductId.Keys, ct);
        var conflictErrors = CollectConflictErrors(aggregatedByProductId.Values, existingSpeakers);

        if (conflictErrors.Count > 0)
        {
            await InsertConflictErrorsAsync(connection, transaction, conflictErrors, ct);

            var invalidRows = await CountDistinctRowsWithErrorsByJobIdAsync(connection, transaction, jobId, ct);
            var validRows = totalRows - invalidRows;

            await UpdateJobAsNeedsFixesAsync(connection, transaction, jobId, totalRows, validRows, invalidRows, ct);
            await transaction.CommitAsync(ct);

            return ImportJobAutoCommitOutcome.ConflictsDetected;
        }

        await UpsertSpeakersAsync(connection, transaction, aggregatedByProductId.Values, existingSpeakers, ct);
        await DeleteStagingRowsAsync(connection, transaction, jobId, ct);
        await UpdateJobAsCommittedAsync(connection, transaction, jobId, DateTimeOffset.UtcNow, ct);
        await transaction.CommitAsync(ct);

        return ImportJobAutoCommitOutcome.Committed;
    }

    private static async Task<List<StagedRowForCommit>> LoadStagedRowsAsync(
        SqliteConnection connection,
        long jobId,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT Id, RowNumber, ProductId, ProductName, ProductRsdValue, ProductQuantity
            FROM ImportRows
            WHERE JobId = @jobId
            ORDER BY RowNumber ASC;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);

        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<StagedRowForCommit>();

        while (await reader.ReadAsync(ct))
        {
            var productIdOrdinal = reader.GetOrdinal("ProductId");
            var productNameOrdinal = reader.GetOrdinal("ProductName");
            var productRsdValueOrdinal = reader.GetOrdinal("ProductRsdValue");
            var productQuantityOrdinal = reader.GetOrdinal("ProductQuantity");

            if (reader.IsDBNull(productIdOrdinal)
                || reader.IsDBNull(productNameOrdinal)
                || reader.IsDBNull(productRsdValueOrdinal)
                || reader.IsDBNull(productQuantityOrdinal))
            {
                throw new InvalidOperationException("Staged row contains null values during commit phase.");
            }

            rows.Add(
                new StagedRowForCommit(
                    reader.GetInt64(reader.GetOrdinal("Id")),
                    reader.GetInt32(reader.GetOrdinal("RowNumber")),
                    reader.GetString(productIdOrdinal),
                    reader.GetString(productNameOrdinal),
                    reader.GetInt32(productRsdValueOrdinal),
                    reader.GetInt32(productQuantityOrdinal)));
        }

        return rows;
    }

    private static Dictionary<string, AggregatedRowForCommit> AggregateByProductId(IReadOnlyList<StagedRowForCommit> stagedRows)
    {
        var aggregatedRows = new Dictionary<string, AggregatedRowForCommit>(StringComparer.Ordinal);

        foreach (var row in stagedRows)
        {
            if (!aggregatedRows.TryGetValue(row.ProductId, out var aggregate))
            {
                aggregatedRows[row.ProductId] = new AggregatedRowForCommit(
                    row.ProductId,
                    row.RowId,
                    row.RowNumber,
                    row.ProductName,
                    row.ProductRsdValue,
                    row.ProductQuantity);
                continue;
            }

            aggregatedRows[row.ProductId] = aggregate with
            {
                LastRowId = row.RowId,
                LastRowNumber = row.RowNumber,
                ProductName = row.ProductName,
                ProductRsdValue = row.ProductRsdValue,
                ProductQuantity = aggregate.ProductQuantity + row.ProductQuantity
            };
        }

        return aggregatedRows;
    }

    private static async Task<Dictionary<string, ExistingSpeakerRow>> LoadExistingSpeakersAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyCollection<string> productIds,
        CancellationToken ct)
    {
        var speakers = new Dictionary<string, ExistingSpeakerRow>(StringComparer.Ordinal);

        if (productIds.Count == 0)
        {
            return speakers;
        }

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;

        var parameterNames = new List<string>(productIds.Count);
        var index = 0;

        foreach (var productId in productIds)
        {
            var parameterName = $"@productId{index}";
            parameterNames.Add(parameterName);
            command.Parameters.AddWithValue(parameterName, productId);
            index++;
        }

        command.CommandText = $"""
            SELECT ProductId, ProductName, ProductRsdValue, ProductQuantity
            FROM Speakers
            WHERE ProductId IN ({string.Join(", ", parameterNames)});
            """;

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var productId = reader.GetString(reader.GetOrdinal("ProductId"));
            speakers[productId] = new ExistingSpeakerRow(
                productId,
                reader.GetString(reader.GetOrdinal("ProductName")),
                reader.GetInt32(reader.GetOrdinal("ProductRsdValue")),
                reader.GetInt32(reader.GetOrdinal("ProductQuantity")));
        }

        return speakers;
    }

    private static List<ConflictErrorForCommit> CollectConflictErrors(
        IReadOnlyCollection<AggregatedRowForCommit> aggregatedRows,
        IReadOnlyDictionary<string, ExistingSpeakerRow> existingSpeakers)
    {
        var errors = new List<ConflictErrorForCommit>();

        foreach (var aggregatedRow in aggregatedRows)
        {
            if (!existingSpeakers.TryGetValue(aggregatedRow.ProductId, out var existingSpeaker))
            {
                continue;
            }

            var productNameConflict = !string.Equals(
                aggregatedRow.ProductName,
                existingSpeaker.ProductName,
                StringComparison.Ordinal);

            var productRsdValueConflict = aggregatedRow.ProductRsdValue != existingSpeaker.ProductRsdValue;

            if (productNameConflict)
            {
                errors.Add(new ConflictErrorForCommit(aggregatedRow.LastRowId, ProductNameField, ProductNameConflictError));
            }

            if (productRsdValueConflict)
            {
                errors.Add(new ConflictErrorForCommit(aggregatedRow.LastRowId, ProductRsdValueField, ProductRsdValueConflictError));
            }
        }

        return errors;
    }

    private static async Task InsertConflictErrorsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyCollection<ConflictErrorForCommit> errors,
        CancellationToken ct)
    {
        foreach (var error in errors)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT OR IGNORE INTO ImportRowErrors (RowId, Field, Error)
                VALUES (@rowId, @field, @error);
                """;
            command.Parameters.AddWithValue("@rowId", error.RowId);
            command.Parameters.AddWithValue("@field", error.Field);
            command.Parameters.AddWithValue("@error", error.Error);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<int> CountDistinctRowsWithErrorsByJobIdAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long jobId,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT COUNT(DISTINCT e.RowId)
            FROM ImportRowErrors e
            INNER JOIN ImportRows r ON r.Id = e.RowId
            WHERE r.JobId = @jobId;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);

        var scalar = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    private static async Task UpdateJobAsNeedsFixesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long jobId,
        int totalRows,
        int validRows,
        int invalidRows,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE ImportJobs
            SET
                Status = @status,
                TotalRows = @totalRows,
                ValidRows = @validRows,
                InvalidRows = @invalidRows
            WHERE Id = @jobId;
            """;
        command.Parameters.AddWithValue("@status", ImportJobStatusDbTokens.ToToken(ImportJobStatus.NeedsFixes));
        command.Parameters.AddWithValue("@totalRows", totalRows);
        command.Parameters.AddWithValue("@validRows", validRows);
        command.Parameters.AddWithValue("@invalidRows", invalidRows);
        command.Parameters.AddWithValue("@jobId", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task UpsertSpeakersAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyCollection<AggregatedRowForCommit> aggregatedRows,
        IReadOnlyDictionary<string, ExistingSpeakerRow> existingSpeakers,
        CancellationToken ct)
    {
        foreach (var aggregatedRow in aggregatedRows)
        {
            if (existingSpeakers.TryGetValue(aggregatedRow.ProductId, out var existingSpeaker))
            {
                await using var updateCommand = connection.CreateCommand();
                updateCommand.Transaction = transaction;
                updateCommand.CommandText = """
                    UPDATE Speakers
                    SET ProductQuantity = @productQuantity
                    WHERE ProductId = @productId;
                    """;
                updateCommand.Parameters.AddWithValue("@productQuantity", existingSpeaker.ProductQuantity + aggregatedRow.ProductQuantity);
                updateCommand.Parameters.AddWithValue("@productId", aggregatedRow.ProductId);
                await updateCommand.ExecuteNonQueryAsync(ct);

                continue;
            }

            await using var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = """
                INSERT INTO Speakers (ProductId, ProductName, ProductRsdValue, ProductQuantity)
                VALUES (@productId, @productName, @productRsdValue, @productQuantity);
                """;
            insertCommand.Parameters.AddWithValue("@productId", aggregatedRow.ProductId);
            insertCommand.Parameters.AddWithValue("@productName", aggregatedRow.ProductName);
            insertCommand.Parameters.AddWithValue("@productRsdValue", aggregatedRow.ProductRsdValue);
            insertCommand.Parameters.AddWithValue("@productQuantity", aggregatedRow.ProductQuantity);
            await insertCommand.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteStagingRowsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long jobId,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DELETE FROM ImportRows
            WHERE JobId = @jobId;
            """;
        command.Parameters.AddWithValue("@jobId", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task UpdateJobAsCommittedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long jobId,
        DateTimeOffset clearedAtUtc,
        CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE ImportJobs
            SET
                Status = @status,
                ClearedAt = @clearedAt
            WHERE Id = @jobId;
            """;
        command.Parameters.AddWithValue("@status", ImportJobStatusDbTokens.ToToken(ImportJobStatus.Committed));
        command.Parameters.AddWithValue("@clearedAt", clearedAtUtc.ToString("O", CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue("@jobId", jobId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private sealed record StagedRowForCommit(
        long RowId,
        int RowNumber,
        string ProductId,
        string ProductName,
        int ProductRsdValue,
        int ProductQuantity);

    private sealed record AggregatedRowForCommit(
        string ProductId,
        long LastRowId,
        int LastRowNumber,
        string ProductName,
        int ProductRsdValue,
        int ProductQuantity);

    private sealed record ExistingSpeakerRow(
        string ProductId,
        string ProductName,
        int ProductRsdValue,
        int ProductQuantity);

    private sealed record ConflictErrorForCommit(
        long RowId,
        string Field,
        string Error);
}

public enum ImportJobAutoCommitOutcome
{
    Committed = 0,
    ConflictsDetected = 1
}
