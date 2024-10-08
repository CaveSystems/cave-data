using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Cave.Data.Sql;

namespace Cave.Data.Sqlite;

/// <summary>Provides a sqlite storage implementation.</summary>
public sealed class SqliteStorage : SqlStorage
{
    #region Private Fields

    const string StaticConnectionString = "Data Source={0}";

    #endregion Private Fields

    #region Private Methods

    /// <summary>Gets the fileName for the specified databaseName name.</summary>
    /// <param name="database">Name of the databaseName (file).</param>
    /// <returns>Full path to the databaseName file.</returns>
    string GetFileName(string database) => Path.GetFullPath(FileSystem.Combine(ConnectionString.Location, database + ".db"));

    #endregion Private Methods

    #region Protected Methods

    /// <inheritdoc/>
    protected override string GetConnectionString(string database)
    {
        if (string.IsNullOrEmpty(database))
        {
            throw new ArgumentNullException(nameof(database));
        }

        var path = GetFileName(database);
        return string.Format(null, StaticConnectionString, path);
    }

    /// <inheritdoc/>
    protected override IDbConnection GetDbConnectionType()
    {
        var flags = AppDom.LoadFlags.NoException;
        var type =
            AppDom.FindType("System.Data.SQLite.SQLiteConnection", "System.Data.SQLite", flags) ??
            AppDom.FindType("Mono.Data.SQLite.SQLiteConnection", "Mono.Data.SQLite", flags);
        if ((type == null) && AllowAssemblyLoad)
        {
            //try with unsafe load
            flags |= AppDom.LoadFlags.LoadAssemblies;
            type =
                AppDom.FindType("System.Data.SQLite.SQLiteConnection", "System.Data.SQLite", flags) ??
                AppDom.FindType("Mono.Data.SQLite.SQLiteConnection", "Mono.Data.SQLite", flags);
        }

        return type == null
            ? throw new TypeLoadException("Could neither load System.Data.SQLite.SQLiteConnection nor Mono.Data.SQLite.SQLiteConnection!")
            : (Activator.CreateInstance(type) as IDbConnection) ?? throw new InvalidOperationException($"CreateInstance({type}) returned null!");
    }

    #endregion Protected Methods

    #region Protected Internal Properties

    /// <inheritdoc/>
    protected internal override bool DBConnectionCanChangeDataBase => false;

    #endregion Protected Internal Properties

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="SqliteStorage"/> class.</summary>
    /// <param name="connectionString">the connection details.</param>
    /// <param name="flags">The connection flags.</param>
    public SqliteStorage(ConnectionString connectionString, ConnectionFlags flags = default)
        : base(connectionString, flags)
    {
    }

    #endregion Public Constructors

    #region Public Properties

    /// <inheritdoc/>
    public override IList<string> DatabaseNames
    {
        get
        {
            var result = new List<string>();
            foreach (var directory in Directory.GetFiles(ConnectionString.Location ?? ".", "*.db"))
            {
                result.Add(Path.GetFileNameWithoutExtension(directory));
            }

            return result;
        }
    }

    /// <inheritdoc/>
    public override string ParameterPrefix => "@";

    /// <inheritdoc/>
    public override bool SupportsAllFieldsGroupBy => true;

    /// <inheritdoc/>
    public override bool SupportsNamedParameters => true;

    /// <inheritdoc/>
    public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1);

    #endregion Public Properties

    #region Public Methods

    /// <summary>
    /// Gets the databaseName type for the specified field type. Sqlite does not implement all different sql92 types directly instead they are reusing only 4
    /// different types. So we have to check only the sqlite value types and convert to the dotnet type.
    /// </summary>
    /// <param name="dataType">Local DataType.</param>
    /// <returns>The databaseName data type to use.</returns>
    /// <exception cref="ArgumentNullException">FieldType.</exception>
    public static DataType GetDatabaseDataType(DataType dataType) => GetValueType(dataType) switch
    {
        SqliteValueType.BLOB => DataType.Binary,
        SqliteValueType.INTEGER => DataType.Int64,
        SqliteValueType.REAL => DataType.Double,
        SqliteValueType.TEXT => DataType.String,
        _ => throw new NotImplementedException($"FieldType {dataType} is not implemented!"),
    };

    /// <summary>Gets the sqlite value type of the specified datatype.</summary>
    /// <param name="dataType">Data type.</param>
    /// <returns>The sqlite value type.</returns>
    public static SqliteValueType GetValueType(DataType dataType) => dataType switch
    {
        DataType.Binary => SqliteValueType.BLOB,
        DataType.Bool or DataType.Enum or DataType.Int8 or DataType.Int16 or DataType.Int32 or DataType.Int64 or DataType.UInt8 or DataType.UInt16 or DataType.UInt32 or DataType.UInt64 => SqliteValueType.INTEGER,
        DataType.DateTime or DataType.Char or DataType.String or DataType.User => SqliteValueType.TEXT,
        DataType.TimeSpan or DataType.Decimal or DataType.Double or DataType.Single => SqliteValueType.REAL,
        _ => throw new NotImplementedException($"DataType {dataType} is not implemented!"),
    };

    /// <inheritdoc/>
    public override IDatabase CreateDatabase(string databaseName)
    {
        var file = GetFileName(databaseName);
        if (File.Exists(file))
        {
            throw new InvalidOperationException($"Database '{databaseName}' already exists!");
        }

        Directory.CreateDirectory(Path.GetDirectoryName(file) ?? throw new InvalidOperationException("No path specified!"));
        File.WriteAllBytes(file, new byte[0]);
        return new SqliteDatabase(this, databaseName);
    }

    /// <inheritdoc/>
    public override void DeleteDatabase(string database)
    {
        if (!HasDatabase(database))
        {
            throw new InvalidOperationException($"Database '{database}' does not exist!");
        }

        File.Delete(GetFileName(database));
    }

    /// <inheritdoc/>
    public override string EscapeFieldName(IFieldProperties field) =>
        field == null ? throw new ArgumentNullException(nameof(field)) : "[" + field.NameAtDatabase + "]";

    /// <inheritdoc/>
    public override string FQTN(string database, string table) => table;

    /// <inheritdoc/>
    public override IDatabase GetDatabase(string databaseName)
    {
        if (!HasDatabase(databaseName))
        {
            throw new InvalidOperationException($"Database '{databaseName}' does not exist!");
        }

        return new SqliteDatabase(this, databaseName);
    }

    /// <inheritdoc/>
    public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
    {
        if (field == null)
        {
            throw new ArgumentNullException(nameof(field));
        }

        var typeAtDatabase = GetDatabaseDataType(field.DataType);
        if (field.TypeAtDatabase != typeAtDatabase)
        {
            var result = field.Clone();
            result.TypeAtDatabase = typeAtDatabase;
            return result;
        }

        return field;
    }

    /// <inheritdoc/>
    public override decimal GetDecimalPrecision(float count) => 0.000000000000001m;

    /// <inheritdoc/>
    public override bool HasDatabase(string databaseName) => File.Exists(GetFileName(databaseName));

    #endregion Public Methods
}
