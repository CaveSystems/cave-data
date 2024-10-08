using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Mysql;

/// <summary>
/// Provides a mysql storage implementation. Attention: <see cref="float"/> variables stored at the mysqldatabase loose their last precision digit (a value of 1
/// may differ by &lt;= 0.000001f).
/// </summary>
public sealed class MysqlStorage : SqlStorage
{
    #region Private Fields

    PropertyInfo? isValidDateTimeProperty;

    #endregion Private Fields

    #region Protected Methods

    /// <inheritdoc/>
    protected override string GetConnectionString(string? database)
    {
        var requireSSL = !AllowUnsafeConnections;
        if (requireSSL)
        {
            if (ConnectionString.Server is "127.0.0.1" or "::1" or "localhost")
            {
                requireSSL = false;
            }
        }

        return
            "Allow User Variables=true;" +
            "Server=" + ConnectionString.Server + ";" +
            "Uid=" + ConnectionString.UserName + ";" +
            "Pwd=" + ConnectionString.Password + ";" +
            "Port=" + ConnectionString.GetPort(3306) + ";" +
            "CharSet=" + (SupportsFullUTF8 ? "utf8mb4" : "utf8") + ";" +
            // "Protocol=socket;" +
            "Allow Zero Datetime=true;" +
            (requireSSL ? "SslMode=Required;" : "SslMode=Preferred;");
    }

    /// <inheritdoc/>
    protected override IDbConnection GetDbConnectionType()
    {
        var flags = AppDom.LoadFlags.NoException;
        var type =
            AppDom.FindType("MySql.Data.MySqlClient.MySqlConnection", "MySql.Data", flags) ??
            AppDom.FindType("MySql.Data.MySqlClient.MySqlConnection", "MySqlConnector", flags) ??
            AppDom.FindType("MySqlConnector.MySqlConnection", "MySqlConnector", flags);
        if ((type == null) && AllowAssemblyLoad)
        {
            //try with unsafe load
            flags |= AppDom.LoadFlags.LoadAssemblies;
            type =
                AppDom.FindType("MySql.Data.MySqlClient.MySqlConnection", "MySql.Data", flags) ??
                AppDom.FindType("MySql.Data.MySqlClient.MySqlConnection", "MySqlConnector", flags) ??
                AppDom.FindType("MySqlConnector.MySqlConnection", "MySqlConnector", flags);
        }

        return type == null
            ? throw new TypeLoadException("Could not load type MySql.Data.MySqlClient.MySqlConnection!")
            : (Activator.CreateInstance(type) as IDbConnection) ?? throw new InvalidOperationException($"CreateInstance({type}) returned null!");
    }

    #endregion Protected Methods

    #region Protected Internal Properties

    /// <inheritdoc/>
    protected internal override bool DBConnectionCanChangeDataBase => true;

    #endregion Protected Internal Properties

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="MysqlStorage"/> class.</summary>
    /// <param name="connectionString">the connection details.</param>
    /// <param name="flags">The connection flags.</param>
    public MysqlStorage(ConnectionString connectionString, ConnectionFlags flags = default)
        : base(connectionString, flags)
    {
        var versionString = (QueryValue("SELECT VERSION()")?.ToString()) ?? throw new InvalidDataException("Could not read mysql version!");
        VersionString = versionString;
        Version = new Version(versionString.BeforeFirst('-'));
        if (Version < new Version(5, 5, 3))
        {
            SupportsFullUTF8 = false;
            CharacterSet = "utf8";
        }
        else
        {
            SupportsFullUTF8 = true;
            CharacterSet = "utf8mb4";
        }

        Trace.TraceInformation($"mysql version <cyan>{Version}<default> supports full utf-8 {(SupportsFullUTF8 ? "<green>" : "<red>") + SupportsFullUTF8}");
        ClearCachedConnections();
    }

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the character set.</summary>
    /// <value>The character set.</value>
    public string CharacterSet { get; } = "utf8";

    /// <inheritdoc/>
    public override IList<string> DatabaseNames
    {
        get
        {
            var result = new List<string>();
            var rows = Query(database: "information_schema", table: "SCHEMATA", cmd: "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA;");
            foreach (var row in rows)
            {
                var db = (row[0]?.ToString()) ?? throw new NullReferenceException("Could not read database names from information_schema!");
                result.Add(db);
            }

            return result;
        }
    }

    /// <summary>Gets the data encoding.</summary>
    /// <value>The data encoding.</value>
    public Encoding DataEncoding { get; } = Encoding.UTF8;

    /// <inheritdoc/>
    public override TimeSpan DateTimePrecision => TimeSpan.FromSeconds(1);

    /// <inheritdoc/>
    public override float FloatPrecision => 0.00001f;

    /// <inheritdoc/>
    public override string ParameterPrefix => "?";

    /// <inheritdoc/>
    public override bool SupportsAllFieldsGroupBy => true;

    /// <summary>Gets a value indicating whether the server instance supports full utf8 or only the 3 byte (BMP characters only) character set.</summary>
    /// <value><c>true</c> if [supports full utf8]; otherwise, <c>false</c>.</value>
    public bool SupportsFullUTF8 { get; }

    /// <inheritdoc/>
    public override bool SupportsNamedParameters => true;

    /// <inheritdoc/>
    public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1);

    /// <summary>Gets the mysql storage version.</summary>
    public Version Version { get; }

    /// <summary>Gets the mysql storage version.</summary>
    public string VersionString { get; }

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override IDatabase CreateDatabase(string databaseName)
    {
        if (databaseName.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Database name contains invalid chars!");
        }

        Execute(database: "information_schema", table: "SCHEMATA",
            cmd: $"CREATE DATABASE `{databaseName}` CHARACTER SET {CharacterSet} COLLATE {CharacterSet}_general_ci;");
        return GetDatabase(databaseName);
    }

    /// <inheritdoc/>
    public override IDbConnection CreateNewConnection(string databaseName)
    {
        var connection = base.CreateNewConnection(databaseName);
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"SET NAMES `{CharacterSet}` COLLATE `{CharacterSet}_unicode_ci`; SET CHARACTER SET `{CharacterSet}`;";
            if (LogVerboseMessages)
            {
                LogQuery(command);
            }

            // return value is not globally defined. some implementations use it correctly, some dont use it, some return positive or negative result enum
            // values so we ignore this: int affectedRows =
            command.ExecuteNonQuery();
        }

        return connection;
    }

    /// <inheritdoc/>
    public override void DeleteDatabase(string database)
    {
        if (database.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Database name contains invalid chars!");
        }

        Execute(database: "information_schema", table: "SCHEMATA", cmd: $"DROP DATABASE {database};");
    }

    /// <inheritdoc/>
    public override string EscapeFieldName(IFieldProperties field) => "`" + field.NameAtDatabase + "`";

    /// <inheritdoc/>
    public override string FQTN(string database, string table) => "`" + database + "`.`" + table + "`";

    /// <inheritdoc/>
    public override IDatabase GetDatabase(string databaseName)
    {
        if (!HasDatabase(databaseName))
        {
            throw new ArgumentException("Database does not exist!");
        }

        return new MysqlDatabase(this, databaseName);
    }

    /// <inheritdoc/>
    public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
    {
        if (field == null)
        {
            throw new ArgumentNullException(nameof(field));
        }

        switch (field.DataType)
        {
            case DataType.DateTime:
                var result = field.Clone();
                result.ValueType = typeof(DateTime);
                return result;
        }

        return base.GetDatabaseFieldProperties(field);
    }

    /// <inheritdoc/>
    public override object? GetDatabaseValue(IFieldProperties field, object? localValue)
    {
        if (field == null)
        {
            throw new ArgumentNullException(nameof(field));
        }

        switch (field.DataType)
        {
            case DataType.String:
            {
                if (localValue == null)
                {
                    return null;
                }

                if (!SupportsFullUTF8)
                {
                    // dirty hack: check mysql 3 byte utf-8
                    var value = (string)localValue;
                    foreach (var c in value)
                    {
                        if (Encoding.UTF8.GetByteCount(new[] { c }) > 3)
                        {
                            throw new NotSupportedException("You can not store 4 byte utf-8 characters to mysql prior version 5.5.3!");
                        }
                    }

                    return DataEncoding.GetBytes(value);
                }

                break;
            }
            case DataType.Double:
            {
                var d = Convert.ToDouble(localValue, Culture);
                return double.IsPositiveInfinity(d) ? double.MaxValue : double.IsNegativeInfinity(d) ? double.MinValue : (object)d;
            }
            case DataType.Single:
            {
                var f = Convert.ToSingle(localValue, Culture);
                if (float.IsPositiveInfinity(f))
                {
                    return float.MaxValue;
                }

                return float.IsNegativeInfinity(f) ? float.MinValue : (object)f;
            }
        }

        return base.GetDatabaseValue(field, localValue);
    }

    /// <inheritdoc/>
    public override decimal GetDecimalPrecision(float count)
    {
        if (count == 0)
        {
            count = 65.30f;
        }

        return base.GetDecimalPrecision(count);
    }

    /// <inheritdoc/>
    public override DataType GetLocalDataType(Type fieldType, uint fieldSize)
    {
        if (fieldType == null)
        {
            throw new ArgumentNullException(nameof(fieldType));
        }

        DataType dataType;
        if (fieldType.Name == "MySqlDateTime")
        {
            // fix mysql date time
            dataType = DataType.DateTime;
        }
        else
        {
            // handle all default types
            dataType = base.GetLocalDataType(fieldType, fieldSize);
            //repair bool (bit) values
            if (fieldSize == 1)
            {
                switch (dataType)
                {
                    case DataType.Int8:
                    case DataType.Int16:
                    case DataType.Int32:
                    case DataType.Int64:
                    case DataType.UInt8:
                    case DataType.UInt16:
                    case DataType.UInt32:
                    case DataType.UInt64:
                        dataType = DataType.Bool;
                        break;
                }
            }
        }

        return dataType;
    }

    /// <inheritdoc/>
    public override object? GetLocalValue(IFieldProperties field, IDataReader reader, object? databaseValue)
    {
        if ((field.DataType == DataType.DateTime) && databaseValue is not null or DBNull or DateTime)
        {
            if (isValidDateTimeProperty == null)
            {
                var type = databaseValue.GetType();
                if (type.Name == "MySqlDateTime")
                {
                    isValidDateTimeProperty = type.GetProperties().Single(p => p.Name == "IsValidDateTime");
                }

                if (isValidDateTimeProperty == null)
                {
                    throw new InvalidDataException($"Unknown data type {type} or missing IsValidDateTime property!");
                }
            }
#if NET20 || NET35 || NET40
            var isValid = isValidDateTimeProperty.GetValue(databaseValue, null);
#else
            var isValid = isValidDateTimeProperty.GetValue(databaseValue);
#endif
            if (isValid is bool valid && valid)
            {
                databaseValue = reader.GetDateTime(field.Index);
            }
            else
            {
                databaseValue = null;
            }
        }

        return base.GetLocalValue(field, reader, databaseValue);
    }

    /// <inheritdoc/>
    public override bool HasDatabase(string databaseName)
    {
        if (databaseName.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Database name contains invalid chars!");
        }

        var value = QueryValue(database: "information_schema", table: "SCHEMATA",
            cmd: "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE " + EscapeString(databaseName) + ";")
            ?? throw new InvalidDataException("Could not read information_schema.tables!");
        return Convert.ToInt32(value, Culture) > 0;
    }

    #endregion Public Methods
}
