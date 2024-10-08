using System;
using Cave.Data.Mssql;
using Cave.Data.Mysql;
using Cave.Data.Pgsql;
using Cave.Data.Sqlite;

namespace Cave.Data;

/// <summary>Connects to different database types.</summary>
public static class Connector
{
    #region Private Fields

    static readonly char[] LocationSeparator = ['/'];

    #endregion Private Fields

    #region Public Methods

    /// <summary>Connects to a database using the specified <see cref="ConnectionString"/>.</summary>
    /// <param name="connection">The ConnectionString.</param>
    /// <param name="options">The database connection options.</param>
    /// <returns>Returns a new database connection.</returns>
    /// <exception cref="ArgumentException">Missing database name at connection string!.</exception>
    public static IDatabase ConnectDatabase(ConnectionString connection, ConnectionFlags options = ConnectionFlags.None)
    {
        var storage = ConnectStorage(connection, options);
        if (connection.Location == null)
        {
            throw new ArgumentOutOfRangeException(nameof(connection), "Database name not specified!");
        }

        var parts = connection.Location.Split(LocationSeparator, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 1)
        {
            throw new ArgumentException("Missing database name at connection string!");
        }

        return storage.GetDatabase(parts[0], (options & ConnectionFlags.AllowCreate) != 0);
    }

    /// <summary>Connects to a database storage.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="options">The options.</param>
    /// <returns>Returns a new storage connection.</returns>
    /// <exception cref="NotSupportedException">Unknown database provider '{connectionString.Protocol}'!.</exception>
    public static IStorage ConnectStorage(ConnectionString connectionString, ConnectionFlags options = 0) => connectionString.ConnectionType switch
    {
        ConnectionType.MEMORY => new MemoryStorage(),
        ConnectionType.MYSQL => new MysqlStorage(connectionString, options),
        ConnectionType.MSSQL => new MssqlStorage(connectionString, options),
        ConnectionType.SQLITE => new SqliteStorage(connectionString, options),
        ConnectionType.PGSQL => new PgsqlStorage(connectionString, options),
        _ => throw new NotSupportedException($"Unknown database provider '{connectionString.Protocol}'!"),
    };

    #endregion Public Methods
}
