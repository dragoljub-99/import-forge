using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace ImportForge.Infrastructure.Db;

public sealed class DbConnectionFactory
{
    private const string SolutionFileName = "ImportForge.sln";

    private readonly string _connectionString;

    public DbConnectionFactory(IConfiguration configuration, IHostEnvironment environment)
    {
        var dataRootSetting = configuration["ImportForge:DataRoot"];
        var dataRootFullPath = ResolveDataRootFullPath(dataRootSetting, environment.ContentRootPath);

        var rawConnectionString = configuration.GetConnectionString("Sqlite")
            ?? throw new InvalidOperationException("Connection string 'Sqlite' is missing.");

        _connectionString = BuildSqliteConnectionString(rawConnectionString, dataRootFullPath);

        EnsureDataDirectoryExists(_connectionString);
    }

    public SqliteConnection CreateConnection()
        => new(_connectionString);

    public async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await ApplyConnectionPragmasAsync(connection, cancellationToken);

        return connection;
    }

    private static string ResolveDataRootFullPath(string? dataRootSetting, string contentRootPath)
    {
        var setting = string.IsNullOrWhiteSpace(dataRootSetting) ? "./data" : dataRootSetting.Trim();

        if (Path.IsPathRooted(setting))
        {
            return Path.GetFullPath(setting);
        }

        var solutionRoot = FindSolutionRoot(contentRootPath) ?? contentRootPath;
        return Path.GetFullPath(setting, solutionRoot);
    }

    private static string? FindSolutionRoot(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);

        while (current is not null)
        {
            var slnPath = Path.Combine(current.FullName, SolutionFileName);
            if (File.Exists(slnPath))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return null;
    }

    private static string BuildSqliteConnectionString(string rawConnectionString, string dataRootFullPath)
    {
        var builder = new SqliteConnectionStringBuilder(rawConnectionString);

        if (string.IsNullOrWhiteSpace(builder.DataSource))
        {
            throw new InvalidOperationException("SQLite connection string must define Data Source.");
        }

        if (builder.DataSource == ":memory:")
        {
            return builder.ToString();
        }

        if (!Path.IsPathRooted(builder.DataSource))
        {
            builder.DataSource = Path.GetFullPath(builder.DataSource, dataRootFullPath);
        }

        return builder.ToString();
    }

    private static void EnsureDataDirectoryExists(string connectionString)
    {
        var builder = new SqliteConnectionStringBuilder(connectionString);
        var dataSource = builder.DataSource;

        if (string.IsNullOrWhiteSpace(dataSource) || dataSource == ":memory:")
        {
            return;
        }

        var fullPath = Path.GetFullPath(dataSource);
        var directory = Path.GetDirectoryName(fullPath);

        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private static async Task ApplyConnectionPragmasAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;
        """;

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
