using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides a base class implementing the <see cref="IDatabase"/> interface.</summary>
public abstract class Database : IDatabase
{
    #region Protected Constructors

    /// <summary>Initializes a new instance of the <see cref="Database"/> class.</summary>
    /// <param name="storage">The storage engine.</param>
    /// <param name="name">The name of the database.</param>
    protected Database(IStorage storage, string name)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    #endregion Protected Constructors

    #region Protected Methods

    /// <summary>Gets the tableName names present at the database.</summary>
    /// <returns>Returns a list of tableName names.</returns>
    protected abstract string[] GetTableNames();

    /// <summary>Logs the tableName layout.</summary>
    /// <param name="layout">The layout.</param>
    protected void LogCreateTable(RowLayout layout)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        Trace.TraceInformation("Creating tableName <cyan>{0}.{1}<default> with <cyan>{2}<default> fields.", Name, layout.Name, layout.FieldCount);
        if (Storage.LogVerboseMessages)
        {
            for (var i = 0; i < layout.FieldCount; i++)
            {
                Trace.TraceInformation(layout[i].ToString());
            }
        }
    }

    #endregion Protected Methods

    #region Public Properties

    /// <inheritdoc/>
    public abstract bool IsClosed { get; }

    /// <inheritdoc/>
    public abstract bool IsSecure { get; }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public IStorage Storage { get; }

    /// <summary>Gets or sets the tableName name cache time.</summary>
    public TimeSpan TableNameCacheTime { get; set; }

    /// <inheritdoc/>
    public StringComparison TableNameComparison { get; protected set; } = StringComparison.InvariantCultureIgnoreCase;

    /// <inheritdoc/>
    public IList<string> TableNames => GetTableNames();

    #endregion Public Properties

    #region Public Indexers

    /// <inheritdoc/>
    public ITable this[string tableName] => GetTable(tableName);

    #endregion Public Indexers

    #region Public Methods

    /// <inheritdoc/>
    public abstract void Close();

    /// <inheritdoc/>
    public abstract ITable CreateTable(RowLayout layout, TableFlags flags = default);

    /// <inheritdoc/>
    public abstract void DeleteTable(string tableName);

    /// <inheritdoc/>
    public IEnumerator<ITable> GetEnumerator() => this.GetTableEnumerator();

    /// <inheritdoc/>
    public abstract ITable GetTable(string tableName, TableFlags flags = default);

    /// <inheritdoc/>
    public ITable GetTable(RowLayout layout, TableFlags flags = default)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        if ((flags & TableFlags.CreateNew) != 0)
        {
            if (HasTable(layout.Name))
            {
                DeleteTable(layout.Name);
            }

            return CreateTable(layout, flags);
        }

        if ((flags & TableFlags.AllowCreate) != 0)
        {
            if (!HasTable(layout.Name))
            {
                return CreateTable(layout, flags);
            }
        }

        var table = GetTable(layout.Name);
        table.UseLayout(layout);
        return table;
    }

    /// <inheritdoc/>
    public virtual bool HasTable(string tableName) => TableNames.Any(t => string.Equals(tableName, t, TableNameComparison));

    /// <inheritdoc/>
    public override string ToString() => Storage.ConnectionString.ChangePath(Name).ToString(ConnectionStringPart.NoCredentials);

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => this.GetTableEnumerator();

    #endregion Public Methods
}
