using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides table name caching for large databases.</summary>
public class CachedDatabase : IDatabase
{
    #region Private Fields

    readonly IDatabase database;
    Dictionary<string, ITable?> tables = new();

    #endregion Private Fields

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="CachedDatabase"/> class.</summary>
    /// <param name="database">Database instance.</param>
    public CachedDatabase(IDatabase database)
    {
        this.database = database;
        Reload();
    }

    #endregion Public Constructors

    #region Public Properties

    /// <inheritdoc/>
    public bool IsClosed => database.IsClosed;

    /// <inheritdoc/>
    public bool IsSecure => database.IsSecure;

    /// <inheritdoc/>
    public string Name => database.Name;

    /// <inheritdoc/>
    public IStorage Storage => database.Storage;

    /// <inheritdoc/>
    public StringComparison TableNameComparison => database.TableNameComparison;

    /// <inheritdoc/>
    public IList<string> TableNames => tables.Keys.ToArray();

    #endregion Public Properties

    #region Public Indexers

    /// <inheritdoc/>
    public ITable this[string tableName] => GetTable(tableName);

    #endregion Public Indexers

    #region Public Methods

    /// <inheritdoc/>
    public void Close() => database.Close();

    /// <inheritdoc/>
    public ITable CreateTable(RowLayout layout, TableFlags flags = TableFlags.None)
    {
        var result = database.CreateTable(layout, flags);
        tables[result.Name] = result;
        return result;
    }

    /// <inheritdoc/>
    public void DeleteTable(string tableName)
    {
        database.DeleteTable(tableName);
        tables.Remove(tableName);
    }

    /// <inheritdoc/>
    public IEnumerator<ITable> GetEnumerator() => this.GetTableEnumerator();

    /// <inheritdoc/>
    public ITable GetTable(string tableName, TableFlags flags = default)
    {
        var cached = tables[tableName];
        if (cached == null)
        {
            tables[tableName] = cached = database.GetTable(tableName, flags);
        }

        if (cached.Flags != flags)
        {
            throw new ArgumentOutOfRangeException(nameof(tableName), $"Table {cached} was already cached with flags {cached.Flags}!");
        }

        return cached;
    }

    /// <inheritdoc/>
    public ITable GetTable(RowLayout layout, TableFlags flags = default) => database.GetTable(layout, flags);

    /// <inheritdoc/>
    public bool HasTable(string tableName) => tables.Keys.Any(t => string.Equals(tableName, t, TableNameComparison));

    /// <summary>Clears the cache and reloads all tablenames.</summary>
    public void Reload()
    {
        var newTables = new Dictionary<string, ITable?>();
        foreach (var table in database.TableNames)
        {
            newTables[table] = null;
        }

        tables = newTables;
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => this.GetTableEnumerator();

    #endregion Public Methods
}
