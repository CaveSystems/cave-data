using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides table name caching for large databases.
    /// </summary>
    public class CachedDatabase : IDatabase
    {
        #region Private Fields

        readonly IDatabase Database;
        Dictionary<string, ITable> tables = new Dictionary<string, ITable>();

        #endregion Private Fields

        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CachedDatabase"/> class.
        /// </summary>
        /// <param name="database">Database instance.</param>
        public CachedDatabase(IDatabase database)
        {
            this.Database = database;
            Reload();
        }

        #endregion Public Constructors

        #region Public Properties

        /// <inheritdoc/>
        public IList<string> TableNames => tables.Keys.ToArray();

        /// <inheritdoc/>
        public bool IsClosed => Database.IsClosed;

        /// <inheritdoc/>
        public bool IsSecure => Database.IsSecure;

        /// <inheritdoc/>
        public string Name => Database.Name;

        /// <inheritdoc/>
        public IStorage Storage => Database.Storage;

        /// <inheritdoc/>
        public StringComparison TableNameComparison => Database.TableNameComparison;

        #endregion Public Properties

        #region Public Indexers

        /// <inheritdoc/>
        public ITable this[string tableName] => GetTable(tableName);

        #endregion Public Indexers

        #region Public Methods

        /// <inheritdoc/>
        public void Close() => Database.Close();

        /// <inheritdoc/>
        public ITable CreateTable(RowLayout layout, TableFlags flags = TableFlags.None)
        {
            var result = Database.CreateTable(layout, flags);
            tables[result.Name] = result;
            return result;
        }

        /// <inheritdoc/>
        public void DeleteTable(string tableName)
        {
            Database.DeleteTable(tableName);
            tables.Remove(tableName);
        }

        /// <inheritdoc/>
        public ITable GetTable(string tableName, TableFlags flags = default)
        {
            var cached = tables[tableName];
            if (cached == null)
            {
                tables[tableName] = cached = Database.GetTable(tableName, flags);
            }

            if (cached.Flags != flags)
            {
                throw new ArgumentOutOfRangeException(nameof(tableName), $"Table {cached} was already cached with flags {cached.Flags}!");
            }

            return cached;
        }

        /// <inheritdoc/>
        public ITable GetTable(RowLayout layout, TableFlags flags = default) => Database.GetTable(layout, flags);

        /// <inheritdoc/>
        public bool HasTable(string tableName) => tables.Keys.Any(t => string.Equals(tableName, t, TableNameComparison));

        /// <summary>
        /// Clears the cache and reloads all tablenames.
        /// </summary>
        public void Reload()
        {
            var newTables = new Dictionary<string, ITable>();
            foreach (var table in Database.TableNames)
            {
                newTables[table] = null;
            }

            tables = newTables;
        }

        #endregion Public Methods
    }
}
