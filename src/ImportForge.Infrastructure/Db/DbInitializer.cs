using Microsoft.Data.Sqlite;

namespace ImportForge.Infrastructure.Db;

public sealed class DbInitializer
{
    private readonly DbConnectionFactory _connectionFactory;

    public DbInitializer(DbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken);
        await CreateSchemaAsync(connection, cancellationToken);
    }

    private static async Task CreateSchemaAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
        CREATE TABLE IF NOT EXISTS Speakers (
            ProductId TEXT NOT NULL PRIMARY KEY
                CHECK (ProductId GLOB '[0-9][0-9][0-9][0-9]'),
            ProductName TEXT NOT NULL,
            ProductRsdValue INTEGER NOT NULL
                CHECK (ProductRsdValue > 0),
            ProductQuantity INTEGER NOT NULL
                CHECK (ProductQuantity >= 0)
        );

        CREATE TABLE IF NOT EXISTS ImportJobs (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Status TEXT NOT NULL,
            TotalRows INTEGER NOT NULL DEFAULT 0,
            ValidRows INTEGER NOT NULL DEFAULT 0,
            InvalidRows INTEGER NOT NULL DEFAULT 0,
            ClearedAt TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS ImportRows (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            JobId INTEGER NOT NULL,
            RowNumber INTEGER NOT NULL,
            ProductId TEXT NULL,
            ProductName TEXT NULL,
            ProductRsdValue INTEGER NULL,
            ProductQuantity INTEGER NULL,
            FOREIGN KEY (JobId) REFERENCES ImportJobs(Id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS ImportRowErrors (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            RowId INTEGER NOT NULL,
            Field TEXT NOT NULL,
            Error TEXT NOT NULL,
            FOREIGN KEY (RowId) REFERENCES ImportRows(Id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS IX_ImportRows_JobId_RowNumber
            ON ImportRows (JobId, RowNumber);

        CREATE INDEX IF NOT EXISTS IX_ImportRows_JobId
            ON ImportRows (JobId);

        CREATE INDEX IF NOT EXISTS IX_ImportRowErrors_RowId
            ON ImportRowErrors (RowId);

        CREATE UNIQUE INDEX IF NOT EXISTS IX_ImportRowErrors_RowId_Field_Error
            ON ImportRowErrors (RowId, Field, Error);
        """;

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
