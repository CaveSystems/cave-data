using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;

namespace Cave.Data;

/// <summary>Provides access to databaseName storage engines.</summary>
public abstract class Storage : IStorage
{
    #region Private Fields

    bool closed;

    #endregion Private Fields

    #region Protected Constructors

    /// <summary>Initializes a new instance of the <see cref="Storage"/> class.</summary>
    /// <param name="connectionString">ConnectionString of the storage.</param>
    /// <param name="flags">The connection flags.</param>
    protected Storage(ConnectionString connectionString, ConnectionFlags flags)
    {
        ConnectionString = connectionString;
        AllowUnsafeConnections = flags.HasFlag(ConnectionFlags.AllowUnsafeConnections);
        LogVerboseMessages = flags.HasFlag(ConnectionFlags.VerboseLogging);
        if (LogVerboseMessages)
        {
            Trace.TraceInformation("Verbose logging <green>enabled!");
        }
    }

    #endregion Protected Constructors

    #region Public Fields

    /// <summary>Epoch DateTime in Ticks.</summary>
    public const long EpochTicks = 621355968000000000;

    #endregion Public Fields

    #region Public Properties

    /// <summary>Allow to load assemblies (unsafe) if desired ado connection type cannot be found in application domain.</summary>
    public static bool AllowAssemblyLoad { get; set; }

    /// <summary>Gets or sets the date time format for big int date time values.</summary>
    public static string BigIntDateTimeFormat { get; set; } = "yyyyMMddHHmmssfff";

    /// <inheritdoc/>
    public bool AllowUnsafeConnections { get; }

    /// <inheritdoc/>
    public virtual bool Closed => closed;

    /// <inheritdoc/>
    public virtual ConnectionString ConnectionString { get; }

    /// <summary>Gets or sets the storage culture.</summary>
    public CultureInfo Culture { get; set; } = CultureInfo.InvariantCulture;

    /// <inheritdoc/>
    public abstract IList<string> DatabaseNames { get; }

    /// <inheritdoc/>
    public virtual TimeSpan DateTimePrecision => TimeSpan.FromMilliseconds(0);

    /// <inheritdoc/>
    public virtual double DoublePrecision => 0;

    /// <inheritdoc/>
    public virtual float FloatPrecision => 0;

    /// <inheritdoc/>
    public bool LogVerboseMessages { get; set; }

    /// <inheritdoc/>
    public abstract bool SupportsNativeTransactions { get; }

    /// <inheritdoc/>
    public virtual TimeSpan TimeSpanPrecision => new(0);

    /// <inheritdoc/>
    public int TransactionRowCount { get; set; } = 5000;

    /// <inheritdoc/>
    string IStorage.BigIntDateTimeFormat => BigIntDateTimeFormat;

    #endregion Public Properties

    #region Public Indexers

    /// <inheritdoc/>
    public IDatabase this[string databaseName] => GetDatabase(databaseName);

    #endregion Public Indexers

    #region Public Methods

    /// <inheritdoc/>
    public virtual void CheckLayout(RowLayout databaseLayout, RowLayout localLayout, TableFlags flags)
    {
        if (databaseLayout == null)
        {
            throw new ArgumentNullException(nameof(databaseLayout));
        }

        if (localLayout == null)
        {
            throw new ArgumentNullException(nameof(localLayout));
        }

        if (flags == 0 || !localLayout.IsTyped)
        {
            RowLayout.CheckLayout(localLayout, databaseLayout);
            return;
        }

        var ignoreMissingFields = flags.HasFlag(TableFlags.IgnoreMissingFields);
        if (!ignoreMissingFields && (databaseLayout.FieldCount != localLayout.FieldCount))
        {
            throw new InvalidDataException($"Field.Count of table {localLayout.Name} != {databaseLayout.Name} differs (found {localLayout.FieldCount} databaseLayout {databaseLayout.FieldCount})!");
        }

        if (!Equals(databaseLayout, localLayout))
        {
            var layout = localLayout.GetMatching(databaseLayout, flags);
            if (layout.FieldCount < localLayout.FieldCount)
            {
                throw new InvalidOperationException("Local Layout contains fields not present at database!");
            }
        }
    }

    /// <inheritdoc/>
    public virtual void Close() => closed = true;

    /// <inheritdoc/>
    public abstract IDatabase CreateDatabase(string databaseName);

    /// <inheritdoc/>
    public abstract void DeleteDatabase(string database);

    /// <inheritdoc/>
    public virtual IDatabase GetDatabase(string databaseName, bool createIfNotExists)
    {
        if (HasDatabase(databaseName))
        {
            return GetDatabase(databaseName);
        }

        if (createIfNotExists)
        {
            return CreateDatabase(databaseName);
        }

        throw new ArgumentException($"The requested databaseName '{databaseName}' was not found!");
    }

    /// <inheritdoc/>
    public abstract IDatabase GetDatabase(string databaseName);

    /// <inheritdoc/>
    public virtual IFieldProperties GetDatabaseFieldProperties(IFieldProperties field) => field;

    /// <inheritdoc/>
    public virtual decimal GetDecimalPrecision(float count)
    {
        if (count == 0)
        {
            return 0;
        }

        var value = Math.Truncate(count);
        var decimalValue = (int)Math.Round((count - value) * 100);
        decimal result = 1;
        while (decimalValue-- > 0)
        {
            result *= 0.1m;
        }

        return result;
    }

    /// <inheritdoc/>
    public abstract bool HasDatabase(string databaseName);

    /// <inheritdoc/>
    public override string ToString() => ConnectionString.ToString(ConnectionStringPart.NoCredentials);

    #endregion Public Methods
}
