using System;
using Cave.Data.Microsoft;
using Cave.Data.Mysql;
using Cave.Data.Postgres;
using Cave.Data.SQLite;

namespace Cave.Data
{
    /// <summary>
    /// Connects to different database types.
    /// </summary>
    public static class Connector
    {
        #region Public Methods

        /// <summary>
        /// Connects to a database using the specified <see cref="ConnectionString"/>.
        /// </summary>
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

            var parts = connection.Location.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 1)
            {
                throw new ArgumentException("Missing database name at connection string!");
            }

            return storage.GetDatabase(parts[0], (options & ConnectionFlags.AllowCreate) != 0);
        }

        /// <summary>
        /// Connects to a database storage.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a new storage connection.</returns>
        /// <exception cref="NotSupportedException">Unknown database provider '{connectionString.Protocol}'!.</exception>
        public static IStorage ConnectStorage(ConnectionString connectionString, ConnectionFlags options = 0) => connectionString.ConnectionType switch
        {
            ConnectionType.MEMORY => new MemoryStorage(),
            ConnectionType.MYSQL => new MySqlStorage(connectionString, options),
            ConnectionType.MSSQL => new MsSqlStorage(connectionString, options),
            ConnectionType.SQLITE => new SQLiteStorage(connectionString, options),
            ConnectionType.PGSQL => new PgSqlStorage(connectionString, options),
            _ => throw new NotSupportedException($"Unknown database provider '{connectionString.Protocol}'!"),
        };

        #endregion Public Methods
    }
}
