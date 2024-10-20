using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Cave.Data.Sql;

/// <summary>Provides a base class for sql 92 <see cref="IStorage"/> implementations.</summary>
public abstract class SqlStorage : Storage, IDisposable
{
    #region Private Fields

    readonly SqlConnectionPool pool;
    bool disposedValue;

    #endregion Private Fields

    #region Private Methods

    /// <summary>Reads a row from a DataReader.</summary>
    /// <param name="layout">The layout.</param>
    /// <param name="reader">The reader.</param>
    /// <returns>A row read from the reader.</returns>
    Row ReadRow(RowLayout layout, IDataReader reader)
    {
        var values = new object?[reader.FieldCount];
#nullable disable
        reader.GetValues(values);
#nullable enable
        try
        {
            foreach (var field in layout)
            {
                if (field is null) throw new NullReferenceException("Null field at layout!");
                var value = values[field.Index];
                values[field.Index] = GetLocalValue(field, reader, value);
            }
        }
        catch (Exception ex)
        {
            throw new InvalidDataException($"Error while reading row data at table {layout}!", ex);
        }

        return new Row(layout, values, false);
    }

    /// <summary>Warns on unsafe connection.</summary>
    void WarnUnsafe()
    {
        if (AllowUnsafeConnections)
        {
            Trace.TraceWarning(
                "<red>AllowUnsafeConnections is enabled!\nConnection details {0} including password and any transmitted data may be seen by any eavesdropper!",
                ConnectionString.ToString(ConnectionStringPart.NoCredentials));
        }
    }

    #endregion Private Methods

    #region Protected Constructors

    /// <summary>Initializes a new instance of the <see cref="SqlStorage"/> class.</summary>
    /// <param name="connectionString">the connection details.</param>
    /// <param name="flags">The connection flags.</param>
    protected SqlStorage(ConnectionString connectionString, ConnectionFlags flags = ConnectionFlags.None)
        : base(connectionString, flags)
    {
        Trace.TraceInformation("Initializing native interop assemblies.");
        using (var dbConnection = GetDbConnectionType())
        using (var cmd = dbConnection.CreateCommand())
        {
            DbConnectionType = dbConnection.GetType();
        }

        pool = new SqlConnectionPool(this);
        WarnUnsafe();
    }

    #endregion Protected Constructors

    #region Protected Methods

    /// <summary>Generates an command for the database connection.</summary>
    /// <param name="connection">The connection the command will be executed at.</param>
    /// <param name="cmd">The sql command.</param>
    /// <returns>A new <see cref="IDbCommand"/> instance.</returns>
    protected virtual IDbCommand CreateCommand(SqlConnection connection, SqlCmd cmd)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (cmd == null)
        {
            throw new ArgumentNullException(nameof(cmd));
        }

        var command = connection.CreateCommand();
        command.CommandText = cmd.Text;
        foreach (var parameter in cmd.Parameters)
        {
            var dataParameter = command.CreateParameter();
            if (SupportsNamedParameters)
            {
                dataParameter.ParameterName = parameter.Name;
            }

            dataParameter.Value = parameter.Value;
            command.Parameters.Add(dataParameter);
        }

        command.CommandTimeout = Math.Max(1, (int)CommandTimeout.TotalSeconds);
        if (LogVerboseMessages)
        {
            LogQuery(command);
        }

        return command;
    }

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
            {
                pool.Close();
            }

            disposedValue = true;
        }
    }

    /// <summary>Gets a connection string for the <see cref="DbConnectionType"/>.</summary>
    /// <param name="database">The database to connect to.</param>
    /// <returns>The connection string for the specified database.</returns>
    protected abstract string GetConnectionString(string database);

    /// <summary>Initializes the needed interop assembly and type and returns an instance.</summary>
    /// <returns>Returns an appropriate <see cref="IDbConnection"/> for this database engine.</returns>
    protected abstract IDbConnection GetDbConnectionType();

    /// <summary>Reads the <see cref="RowLayout"/> from an <see cref="IDataReader"/> containing a query result.</summary>
    /// <param name="reader">The reader to read from.</param>
    /// <param name="source">Name of the source.</param>
    /// <returns>A layout generated from the specified <paramref name="reader"/> using <paramref name="source"/> as name.</returns>
    protected virtual RowLayout ReadSchema(IDataReader reader, string source)
    {
        if (reader == null)
        {
            throw new ArgumentNullException(nameof(reader));
        }

        // check columns (name, number and type)
        var schemaTable = reader.GetSchemaTable() ?? throw new NullReferenceException("GetSchemaTable() returned null!");
        var fieldCount = reader.FieldCount;
        var fields = new IFieldProperties[fieldCount];

        // check fieldcount
        if (fieldCount != schemaTable.Rows.Count)
        {
            throw new InvalidDataException($"Invalid field count at SchemaTable!");
        }

        for (var i = 0; i < fieldCount; i++)
        {
            var row = schemaTable.Rows[i];
            var isHidden = row["IsHidden"];
            if ((isHidden != DBNull.Value) && (bool)isHidden)
            {
                // continue;
            }

            var fieldName = (string)row["ColumnName"];
            if (string.IsNullOrEmpty(fieldName))
            {
                fieldName = $"{i}";
            }

            var fieldSize = (uint)(int)row["ColumnSize"];
            var valueType = reader.GetFieldType(i);
            var dataType = GetLocalDataType(valueType, fieldSize);
            var fieldFlags = FieldFlags.None;
            var isKey = row["IsKey"];
            if ((isKey != DBNull.Value) && (bool)isKey)
            {
                fieldFlags |= FieldFlags.ID;
            }

            var isAutoIncrement = row["IsAutoIncrement"];
            if ((isAutoIncrement != DBNull.Value) && (bool)isAutoIncrement)
            {
                fieldFlags |= FieldFlags.AutoIncrement;
            }

            var isUnique = row["IsUnique"];
            if ((isUnique != DBNull.Value) && (bool)isUnique)
            {
                fieldFlags |= FieldFlags.Unique;
            }

            var isNullable = row["AllowDBNull"];
            if ((isNullable != DBNull.Value) && (bool)isNullable)
            {
                fieldFlags |= FieldFlags.Nullable;
            }

            // TODO detect bigint timestamps TODO detect string encoding
            var properties = new FieldProperties
            {
                Index = i,
                Flags = fieldFlags,
                DataType = dataType,
                ValueType = valueType,
                MaximumLength = fieldSize,
                Name = fieldName,
                TypeAtDatabase = dataType,
                NameAtDatabase = fieldName,
            };
            fields[i] = GetDatabaseFieldProperties(properties);
        }

        return RowLayout.CreateUntyped(source, fields);
    }

    /// <summary>Updates a <see cref="RowLayout"/> result from <see cref="QuerySchema(string, string)"/> with additional data from information_schema.</summary>
    /// <param name="layout">Layout to update</param>
    /// <param name="database">Database name</param>
    /// <param name="table">Table name</param>
    /// <returns>Returns the updated row layout</returns>
    protected virtual RowLayout UpdateSchema(RowLayout layout, string database, string table)
    {
        var command = new SqlCommandBuilder(this);
        command.Append("SELECT * FROM ");
        command.Append(FQTN("information_schema", "COLUMNS"));
        command.Append(" WHERE table_name = ");
        command.CreateAndAddParameter(table);
        command.Append(" AND table_schema = ");
        command.CreateAndAddParameter(database);
        var rows = Query(command).ToDictionary(r => r["COLUMN_NAME"]!.ToString()!);
        FieldProperties UpdateProperties(IFieldProperties fieldProperties)
        {
            var row = rows[fieldProperties.NameAtDatabase];
            var field = fieldProperties.Clone();
            field.Description = (string?)row["COLUMN_COMMENT"];
            return field;
        }
        var fields = layout.Fields.Select(UpdateProperties).ToArray();
        return new RowLayout(layout.Name, fields, layout.RowType);
    }

    #endregion Protected Methods

    #region Protected Internal Properties

    /// <summary>Gets a value indicating whether the db connections can change the database with the Sql92 "USE Database" command.</summary>
    protected internal abstract bool DBConnectionCanChangeDataBase { get; }

    /// <summary>Gets the <see cref="IDbConnection"/> type.</summary>
    protected internal Type DbConnectionType { get; }

    #endregion Protected Internal Properties

    #region Protected Internal Methods

    /// <summary>Logs the query in verbose mode.</summary>
    /// <param name="command">The command.</param>
    protected internal static void LogQuery(IDbCommand command)
    {
        if (command == null)
        {
            throw new ArgumentNullException(nameof(command));
        }

        if (command.Parameters.Count > 0)
        {
            var paramText = new StringBuilder();
            foreach (IDataParameter dp in command.Parameters)
            {
                if (paramText.Length > 0)
                {
                    paramText.Append(',');
                }

                paramText.Append(dp.Value);
            }

            Trace.TraceInformation("Execute sql statement:\n<cyan>{0}\nParameters:\n<magenta>{1}", command.CommandText, paramText);
        }
        else
        {
            Trace.TraceInformation("Execute sql statement: <cyan>{0}", command.CommandText);
        }
    }

    #endregion Protected Internal Methods

    #region Public Properties

    /// <summary>Gets or sets command timeout for all sql commands.</summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Gets or sets the connection close timeout.</summary>
    /// <value>The connection close timeout.</value>
    public TimeSpan ConnectionCloseTimeout { get => pool.ConnectionCloseTimeout; set => pool.ConnectionCloseTimeout = value; }

    /// <summary>Gets or sets the default <see cref="DateTimeKind"/> used when reading date fields without explicit definition.</summary>
    public DateTimeKind DefaultDateTimeKind { get; set; } = DateTimeKind.Local;

    /// <summary>Perform a check of the database schema on each query (defaults to false).</summary>
    /// <remarks>
    /// This impacts performance very badly if you query large amounts of single rows. A common practice is to use this while developing the application and
    /// unittest, running the unittests and set this to false on release builds.
    /// </remarks>
    public bool DoSchemaCheckOnQuery { get; set; } = Debugger.IsAttached;

    /// <summary>Gets or sets the maximum error retries.</summary>
    /// <remarks>
    /// If set to &lt; 1 only a single try is made to execute a query. If set to any number &gt; 0 this values indicates the number of retries that are made
    /// after the first try and subsequent tries fail.
    /// </remarks>
    /// <value>The maximum error retries.</value>
    public int MaxErrorRetries { get; set; } = 3;

    /// <summary>Gets or sets the native date time format.</summary>
    /// <value>The native date time format.</value>
    public string NativeDateTimeFormat { get; set; } = "yyyy'-'MM'-'dd' 'HH':'mm':'ss";

    /// <summary>Gets or sets the command(s) to be run for each newly created connection.</summary>
    /// <remarks>After changing this you can use <see cref="ClearCachedConnections()"/> to force reconnecting.</remarks>
    public SqlCmd? NewConnectionCommand { get; set; }

    /// <summary>Gets the parameter prefix for this storage.</summary>
    public abstract string ParameterPrefix { get; }

    /// <summary>Gets a value indicating whether the connection supports select * groupby.</summary>
    public abstract bool SupportsAllFieldsGroupBy { get; }

    /// <summary>Gets a value indicating whether the connection supports named parameters or not.</summary>
    public abstract bool SupportsNamedParameters { get; }

    /// <summary>Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.</summary>
    /// <value><c>true</c> if supports native transactions; otherwise, <c>false</c>.</value>
    public override bool SupportsNativeTransactions { get; } = true;

    /// <summary>Gets or sets a value indicating whether we throw an <see cref="InvalidDataException"/> on date time field conversion errors.</summary>
    public bool ThrowDateTimeFieldExceptions { get; set; }

    #endregion Public Properties

    #region Public Methods

    /// <summary>Closes and clears all cached connections.</summary>
    /// <exception cref="ObjectDisposedException">SqlConnection.</exception>
    public void ClearCachedConnections()
    {
        if (pool == null)
        {
            throw new ObjectDisposedException("SqlConnection");
        }

        pool.Clear();
    }

    /// <summary>closes the connection to the storage engine.</summary>
    public override void Close() => Dispose();

    /// <summary>Creates a new database connection.</summary>
    /// <param name="databaseName">Name of the database.</param>
    /// <returns>A new <see cref="IDbConnection"/> instance.</returns>
    public virtual IDbConnection CreateNewConnection(string databaseName)
    {
        var connection = (Activator.CreateInstance(DbConnectionType) as IDbConnection) ?? throw new InvalidOperationException($"CreateInstance({DbConnectionType}) returned null!");
        connection.ConnectionString = GetConnectionString(databaseName);
        connection.Open();
        WarnUnsafe();
        if (NewConnectionCommand != null)
        {
            using var command = connection.CreateCommand();
            command.CommandText = NewConnectionCommand;
            if (LogVerboseMessages)
            {
                LogQuery(command);
            }

            command.ExecuteNonQuery();
        }

        if (DBConnectionCanChangeDataBase)
        {
            if ((databaseName != string.Empty) && (connection.Database != databaseName))
            {
                connection.ChangeDatabase(databaseName);
            }
        }

        return connection;
    }

    /// <summary>Releases all resources used by the SqlConnection.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Escapes the given binary data.</summary>
    /// <param name="data">The data.</param>
    /// <returns>The escaped binary data.</returns>
    public virtual string EscapeBinary(byte[] data) => "X'" + data.ToHexString() + "'";

    /// <summary>Escapes a field name for direct use in a query.</summary>
    /// <param name="field">The field.</param>
    /// <returns>The escaped field.</returns>
    public abstract string EscapeFieldName(IFieldProperties field);

    /// <summary>Escapes a field value for direct use in a query (whenever possible use parameters instead!).</summary>
    /// <param name="properties">FieldProperties.</param>
    /// <param name="value">Value to escape.</param>
    /// <returns>The escaped field value.</returns>
    public virtual string EscapeFieldValue(IFieldProperties properties, object? value)
    {
        if (properties == null) { throw new ArgumentNullException(nameof(properties)); }

        switch (value)
        {
            case null:
                return "NULL";

            case byte[] bytes:
                return EscapeBinary(bytes);

            case byte:
            case sbyte:
            case ushort:
            case short:
            case uint:
            case int:
            case long:
            case ulong:
            case decimal:
                return value.ToString()!;

            case double d:
                return d.ToString("R", Culture);

            case float f:
                return f.ToString("R", Culture);

            case bool b:
                return b ? "1" : "0";

            //case TimeSpan:
            //case DateTime:
            //case user
            //case enum
            default:
            {
                return EscapeString(Fields.GetString(value, properties.DataType, properties.DateTimeKind, properties.DateTimeType, provider: Culture));
            }
        }
    }

    /// <summary>Escapes a string value for direct use in a query (whenever possible use parameters instead!).</summary>
    /// <param name="text">Text to escape.</param>
    /// <returns>The escaped text.</returns>
    public virtual string EscapeString(string text)
    {
        if (text == null)
        {
            throw new ArgumentNullException(nameof(text));
        }

        // escape escape char
        if (text.IndexOf('\\') != -1)
        {
            text = text.Replace("\\", "\\\\");
        }

        // escape invalid chars
        if (text.IndexOf('\0') != -1)
        {
            text = text.Replace("\0", "\\0");
        }

        if (text.IndexOf('\'') != -1)
        {
            text = text.Replace("'", "\\'");
        }

        if (text.IndexOf('"') != -1)
        {
            text = text.Replace("\"", "\\\"");
        }

        if (text.IndexOf('\b') != -1)
        {
            text = text.Replace("\b", "\\b");
        }

        if (text.IndexOf('\n') != -1)
        {
            text = text.Replace("\n", "\\n");
        }

        if (text.IndexOf('\r') != -1)
        {
            text = text.Replace("\r", "\\r");
        }

        if (text.IndexOf('\t') != -1)
        {
            text = text.Replace("\t", "\\t");
        }

        return "'" + text + "'";
    }

    /// <summary>Executes a database dependent sql statement silently.</summary>
    /// <param name="cmd">the database dependent sql statement.</param>
    /// <param name="database">The affected database (optional, used to get a cached connection).</param>
    /// <param name="table">The affected table (optional, used to get a cached connection).</param>
    /// <returns>Number of affected rows (if supported by the database).</returns>
    public virtual int Execute(SqlCmd cmd, string? database = null, string? table = null)
    {
        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        for (var i = 1; ; i++)
        {
            var connection = GetConnection(database ?? string.Empty);
            var error = false;
            try
            {
                using var command = CreateCommand(connection, cmd);
                if (LogVerboseMessages)
                {
                    LogQuery(command);
                }

                return command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                error = true;
                if ((connection.State == ConnectionState.Open) || (i > MaxErrorRetries))
                {
                    throw;
                }

                Trace.TraceError("<red>{3}<default> Error during Execute(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database,
                    table, i, ex.Message, ex);
            }
            finally
            {
                ReturnConnection(ref connection, error);
            }
        }
    }

    /// <summary>Gets a full qualified table name.</summary>
    /// <param name="database">A database name.</param>
    /// <param name="table">A table name.</param>
    /// <returns>The full qualified table name.</returns>
    public abstract string FQTN(string database, string table);

    /// <summary>Retrieves a connection (from the cache) or creates a new one if needed.</summary>
    /// <param name="databaseName">Name of the database.</param>
    /// <returns>A connection for the specified database.</returns>
    public SqlConnection GetConnection(string databaseName) => pool.GetConnection(databaseName);

    /// <summary>Gets FieldProperties for the Database based on requested FieldProperties.</summary>
    /// <param name="field">The field.</param>
    /// <returns>A new <see cref="FieldProperties"/> instance.</returns>
    public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
    {
        if (field == null)
        {
            throw new ArgumentNullException(nameof(field));
        }

        // check if datatype is replacement for missing sql type
        switch (field.DataType)
        {
            case DataType.Enum:
            {
                var result = field.Clone();
                result.TypeAtDatabase = DataType.Int64;
                return result;
            }
            case DataType.User:
            {
                var result = field.Clone();
                result.TypeAtDatabase = DataType.String;
                return result;
            }
            case DataType.DateTime:
            case DataType.TimeSpan:
                switch (field.DateTimeType)
                {
                    case DateTimeType.Undefined:
                    case DateTimeType.Native:
                    {
                        return field;
                    }
                    case DateTimeType.BigIntHumanReadable:
                    case DateTimeType.BigIntTicks:
                    case DateTimeType.BigIntSeconds:
                    case DateTimeType.BigIntMilliSeconds:
                    case DateTimeType.BigIntEpoch:
                    {
                        var result = field.Clone();
                        result.TypeAtDatabase = DataType.Int64;
                        return result;
                    }
                    case DateTimeType.DecimalSeconds:
                    {
                        var result = field.Clone();
                        result.TypeAtDatabase = DataType.Decimal;
                        result.MaximumLength = 65.3f;
                        return result;
                    }
                    case DateTimeType.DoubleEpoch:
                    case DateTimeType.DoubleSeconds:
                    {
                        var result = field.Clone();
                        result.TypeAtDatabase = DataType.Double;
                        return result;
                    }
                    default: throw new NotImplementedException();
                }
        }

        return field;
    }

    /// <summary>Converts a local value into a database value.</summary>
    /// <param name="field">The <see cref="FieldProperties"/> of the affected field.</param>
    /// <param name="localValue">The local value to be encoded for the database.</param>
    /// <returns>The value as database value type.</returns>
    public virtual object? GetDatabaseValue(IFieldProperties field, object? localValue)
    {
        try
        {
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (localValue == null)
            {
                return null;
            }

            switch (field.DataType)
            {
                case DataType.Guid:
                    if (field.TypeAtDatabase == DataType.String)
                    {
                        return (localValue as Guid?)?.ToString("D");
                    }
                    return localValue;

                case DataType.Enum:
                    return Convert.ToInt64(localValue, Culture);

                case DataType.User:
                    return localValue.ToString();

                case DataType.TimeSpan:
                {
                    var value = (TimeSpan)localValue;
                    return field.DateTimeType switch
                    {
                        DateTimeType.Undefined or DateTimeType.Native => value,
                        DateTimeType.BigIntHumanReadable => value.Ticks < 0 ? -long.Parse(new DateTime(-value.Ticks).ToString(BigIntDateTimeFormat, Culture), Culture) : long.Parse(new DateTime(value.Ticks).ToString(BigIntDateTimeFormat, Culture), Culture),
                        DateTimeType.BigIntTicks => value.Ticks,
                        DateTimeType.BigIntSeconds => value.Ticks / TimeSpan.TicksPerSecond,
                        DateTimeType.BigIntMilliSeconds => value.Ticks / TimeSpan.TicksPerMillisecond,
                        DateTimeType.DecimalSeconds => (decimal)value.Ticks / TimeSpan.TicksPerSecond,
                        DateTimeType.DoubleSeconds => (double)value.Ticks / TimeSpan.TicksPerSecond,
                        DateTimeType.BigIntEpoch or DateTimeType.DoubleEpoch => throw new NotSupportedException($"DateTimeType {field.DateTimeType} not supported at {field.DataType} field!"),
                        _ => throw new NotImplementedException($"DateTimeType {field.DateTimeType} not implemented!"),
                    };
                }
                case DataType.DateTime:
                {
                    if ((DateTime)localValue == default)
                    {
                        return null;
                    }

                    var value = (DateTime)localValue;
                    switch (field.DateTimeKind)
                    {
                        case DateTimeKind.Unspecified: break;
                        case DateTimeKind.Local:
                            if (value.Kind == DateTimeKind.Utc)
                            {
                                value = value.ToLocalTime();
                            }
                            else
                            {
                                value = new DateTime(value.Ticks, DateTimeKind.Local);
                            }

                            break;

                        case DateTimeKind.Utc:
                            if (value.Kind == DateTimeKind.Local)
                            {
                                value = value.ToUniversalTime();
                            }
                            else
                            {
                                value = new DateTime(value.Ticks, DateTimeKind.Utc);
                            }

                            break;

                        default:
                            throw new NotSupportedException($"DateTimeKind {field.DateTimeKind} not supported!");
                    }

                    return field.DateTimeType switch
                    {
                        DateTimeType.Undefined or DateTimeType.Native => value,
                        DateTimeType.BigIntHumanReadable => long.Parse(value.ToString(BigIntDateTimeFormat, Culture), Culture),
                        DateTimeType.BigIntTicks => value.Ticks,
                        DateTimeType.BigIntSeconds => value.Ticks / TimeSpan.TicksPerSecond,
                        DateTimeType.BigIntMilliSeconds => value.Ticks / TimeSpan.TicksPerMillisecond,
                        DateTimeType.BigIntEpoch => (value.Ticks - EpochTicks) / TimeSpan.TicksPerSecond,
                        DateTimeType.DecimalSeconds => value.Ticks / (decimal)TimeSpan.TicksPerSecond,
                        DateTimeType.DoubleSeconds => value.Ticks / (double)TimeSpan.TicksPerSecond,
                        DateTimeType.DoubleEpoch => (value.Ticks - EpochTicks) / (double)TimeSpan.TicksPerSecond,
                        _ => throw new NotImplementedException(),
                    };
                }
            }

            return localValue;
        }
        catch
        {
            throw new ArgumentException($"Invalid value at field {field}!");
        }
    }

    /// <summary>Obtains the local <see cref="DataType"/> for the specified database fieldtype.</summary>
    /// <param name="fieldType">The field type at the database.</param>
    /// <param name="fieldSize">The field size at the database.</param>
    /// <returns>The local csharp datatype.</returns>
    public virtual DataType GetLocalDataType(Type fieldType, uint fieldSize) => RowLayout.DataTypeFromType(fieldType);

    /// <summary>Converts a database value into a local value.</summary>
    /// <param name="field">The <see cref="FieldProperties"/> of the affected field.</param>
    /// <param name="reader">The reader to read values from.</param>
    /// <param name="databaseValue">The value at the database.</param>
    /// <returns>Returns a value as local csharp value type.</returns>
    public virtual object? GetLocalValue(IFieldProperties field, IDataReader reader, object? databaseValue)
    {
        if (field == null) throw new ArgumentNullException(nameof(field));
        if (reader == null) throw new ArgumentNullException(nameof(reader));
        if (databaseValue is DBNull or null) return null;
        if (field.ValueType is null) throw new InvalidOperationException("Field.ValueType has to be set!");

        switch (field.DataType)
        {
            case DataType.Int8: return databaseValue is sbyte int8 ? int8 : Convert.ToSByte(databaseValue);
            case DataType.Int16: return databaseValue is short int16 ? int16 : Convert.ToInt16(field.Index);
            case DataType.Int32: return databaseValue is int int32 ? int32 : Convert.ToInt32(databaseValue);
            case DataType.Int64: return databaseValue is long int64 ? int64 : Convert.ToInt64(databaseValue);
            case DataType.UInt8: return databaseValue is byte uint8 ? uint8 : Convert.ToByte(databaseValue);
            case DataType.UInt16: return databaseValue is ushort uint16 ? uint16 : Convert.ToUInt16(databaseValue);
            case DataType.UInt32: return databaseValue is uint uint32 ? uint32 : Convert.ToUInt32(databaseValue);
            case DataType.UInt64: return databaseValue is ulong uint64 ? uint64 : Convert.ToUInt64(databaseValue);
            case DataType.String: return databaseValue is string str ? str : throw new InvalidOperationException("Could not retrieve string!");
            case DataType.Binary: return databaseValue is byte[] buffer ? buffer : throw new InvalidOperationException("Could not retrieve data!");
            case DataType.Bool: return databaseValue is bool b ? b : Convert.ToBoolean(databaseValue);
            case DataType.Char: return databaseValue is char c ? c : Convert.ToChar(databaseValue);
            case DataType.Decimal: return databaseValue is decimal dec ? dec : Convert.ToDecimal(databaseValue);
            case DataType.Double: return databaseValue is double d ? d : Convert.ToDouble(databaseValue);
            case DataType.Single: return databaseValue is float f ? f : Convert.ToSingle(databaseValue);
            case DataType.User: return field.ParseValue($"{databaseValue}", null, Culture);
            case DataType.Enum: return Enum.ToObject(field.ValueType, reader.GetInt64(field.Index));
            case DataType.Guid: return databaseValue is Guid guid ? guid : new Guid($"{databaseValue}");
            case DataType.DateTime:
            {
                long ticks = 0;
                switch (field.DateTimeType)
                {
                    default: throw new NotSupportedException($"DateTimeType {field.DateTimeType} is not supported");
                    case DateTimeType.BigIntHumanReadable:
                    {
                        var text = ((long)databaseValue).ToString(Culture);
                        ticks = DateTime.ParseExact(text, BigIntDateTimeFormat, Culture).Ticks;
                        break;
                    }
                    case DateTimeType.Undefined:
                    case DateTimeType.Native:
                        try
                        {
                            if (databaseValue is DateTime dt)
                            {
                                ticks = dt.Ticks;
                            }
                            else
                            {
                                ticks = reader.GetDateTime(field.Index).Ticks;
                            }
                        }
                        catch (Exception ex)
                        {
                            var msg = $"Invalid datetime value {reader.GetValue(field.Index)} at {field}.";
                            Trace.WriteLine(msg);
                            if (ThrowDateTimeFieldExceptions)
                            {
                                throw new InvalidDataException(msg, ex);
                            }
                        }

                        break;

                    case DateTimeType.BigIntTicks:
                        ticks = (long)databaseValue;
                        break;

                    case DateTimeType.BigIntSeconds:
                        ticks = (long)databaseValue * TimeSpan.TicksPerSecond;
                        break;

                    case DateTimeType.BigIntMilliSeconds:
                        ticks = (long)databaseValue * TimeSpan.TicksPerMillisecond;
                        break;

                    case DateTimeType.BigIntEpoch:
                        ticks = ((long)databaseValue * TimeSpan.TicksPerSecond) + EpochTicks;
                        break;

                    case DateTimeType.DecimalSeconds:
                        ticks = (long)decimal.Round((decimal)databaseValue * TimeSpan.TicksPerSecond);
                        break;

                    case DateTimeType.DoubleSeconds:
                        ticks = (long)Math.Round((double)databaseValue * TimeSpan.TicksPerSecond);
                        break;

                    case DateTimeType.DoubleEpoch:
                        ticks = (long)Math.Round(((double)databaseValue * TimeSpan.TicksPerSecond) + EpochTicks);
                        break;
                }

                var kind = field.DateTimeKind != 0 ? field.DateTimeKind : DefaultDateTimeKind;
                return new DateTime(ticks, kind);
            }
            case DataType.TimeSpan:
            {
                long ticks;
                switch (field.DateTimeType)
                {
                    default: throw new NotSupportedException($"DateTimeType {field.DateTimeType} is not supported");
                    case DateTimeType.BigIntHumanReadable:
                    {
                        var val = (long)databaseValue;
                        var text = Math.Abs(val).ToString(Culture);
                        ticks = DateTime.ParseExact(text, BigIntDateTimeFormat, Culture).Ticks;
                        if (val < 0) ticks = -ticks;
                        break;
                    }
                    case DateTimeType.Undefined:
                    case DateTimeType.Native:
                        ticks = ((TimeSpan)Convert.ChangeType(databaseValue, typeof(TimeSpan), Culture)).Ticks;
                        break;

                    case DateTimeType.BigIntTicks:
                        ticks = (long)databaseValue;
                        break;

                    case DateTimeType.BigIntSeconds:
                        ticks = (long)databaseValue * TimeSpan.TicksPerSecond;
                        break;

                    case DateTimeType.BigIntMilliSeconds:
                        ticks = (long)databaseValue * TimeSpan.TicksPerMillisecond;
                        break;

                    case DateTimeType.BigIntEpoch:
                        ticks = ((long)databaseValue * TimeSpan.TicksPerSecond) + EpochTicks;
                        break;

                    case DateTimeType.DecimalSeconds:
                        ticks = (long)decimal.Round((decimal)databaseValue * TimeSpan.TicksPerSecond);
                        break;

                    case DateTimeType.DoubleSeconds:
                        ticks = (long)Math.Round((double)databaseValue * TimeSpan.TicksPerSecond);
                        break;

                    case DateTimeType.DoubleEpoch:
                        ticks = (long)Math.Round(((double)databaseValue * TimeSpan.TicksPerSecond) + EpochTicks);
                        break;
                }

                return new TimeSpan(ticks);
            }
        }

        if (field.DataType != field.TypeAtDatabase)
        {
            //local type does not match database.... try to convert...
            return Convert.ChangeType(databaseValue, field.ValueType);
        }

        // fallback
        {
            return databaseValue;
        }
    }

    /// <summary>Queries for all matching datasets.</summary>
    /// <param name="cmd">The database dependent sql statement.</param>
    /// <param name="database">The databasename (optional, used with cached connections).</param>
    /// <param name="table">The tablename (optional, used with cached connections).</param>
    /// <returns>The result rows.</returns>
    public IList<Row> Query(SqlCmd cmd, string? database = null, string? table = null)
    {
        RowLayout? layout = null;
        return Query(cmd, ref layout, database, table);
    }

    /// <summary>Queries for all matching datasets.</summary>
    /// <param name="cmd">The database dependent sql statement.</param>
    /// <param name="layout">The expected schema layout (if unset the layout is returned).</param>
    /// <param name="database">The databasename (optional, used with cached connections).</param>
    /// <param name="table">The tablename (optional, used with cached connections).</param>
    /// <returns>The result rows.</returns>
    public virtual IList<Row> Query(SqlCmd cmd, ref RowLayout? layout, string? database = null, string? table = null)
    {
        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        table ??= "result";

        // get command
        for (var i = 1; ; i++)
        {
            var connection = GetConnection(database ?? string.Empty);
            var error = false;
            try
            {
                using var command = CreateCommand(connection, cmd);
                if (LogVerboseMessages)
                {
                    LogQuery(command);
                }

                using var reader = command.ExecuteReader(CommandBehavior.KeyInfo);
                // load schema
                var schema = ReadSchema(reader, table);

                // layout specified ?
                if (layout == null)
                {
                    // no: use schema
                    layout = schema;
                }
                else if (DoSchemaCheckOnQuery)
                {
                    // yes: check schema
                    CheckLayout(schema, layout, TableFlags.IgnoreMissingFields);
                }

                // load rows
                var result = new List<Row>();
                while (reader.Read())
                {
                    var row = ReadRow(layout, reader);
                    result.Add(row);
                }

                return result;
            }
            catch (Exception ex)
            {
                error = true;
                if (i > MaxErrorRetries)
                {
                    throw;
                }

                Trace.TraceError("<red>{3}<default> Error during Query(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table,
                    i, ex.Message, ex);
            }
            finally
            {
                ReturnConnection(ref connection, error);
            }
        }
    }

    /// <summary>Queries for a dataset (selected fields, one row).</summary>
    /// <param name="cmd">the database dependent sql statement.</param>
    /// <param name="layout">The expected layout.</param>
    /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
    /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
    /// <returns>The result row.</returns>
    public Row QueryRow(SqlCmd cmd, ref RowLayout? layout, string? database = null, string? table = null) => Query(cmd, ref layout, database, table).Single();

    /// <summary>Gets the <see cref="RowLayout"/> of the specified database table.</summary>
    /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
    /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
    /// <returns>A layout for the specified table.</returns>
    public virtual RowLayout QuerySchema(string database, string table)
    {
        if (database == null)
        {
            throw new ArgumentNullException(nameof(database));
        }

        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        for (var i = 1; ; i++)
        {
            var connection = GetConnection(database);
            var error = false;
            try
            {
                var fqtn = FQTN(database, table);
                using var command = CreateCommand(connection, $"SELECT * FROM {fqtn} WHERE FALSE");
                if (LogVerboseMessages)
                {
                    LogQuery(command);
                }

                using var reader = command.ExecuteReader(CommandBehavior.KeyInfo);
                var schema = ReadSchema(reader, table);
                return UpdateSchema(schema, database, table);
            }
            catch (Exception ex)
            {
                error = true;
                if (i > MaxErrorRetries)
                {
                    throw;
                }

                Trace.TraceError("<red>{3}<default> Error during QuerySchema(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database,
                    table, i, ex.Message, ex);
            }
            finally
            {
                ReturnConnection(ref connection, error);
            }
        }
    }

    /// <summary>Queries for all matching datasets.</summary>
    /// <param name="cmd">The database dependent sql statement.</param>
    /// <param name="database">The databasename (optional, used with cached connections).</param>
    /// <param name="table">The tablename (optional, used with cached connections).</param>
    /// <returns>The result rows.</returns>
    /// <typeparam name="TStruct">Result row type.</typeparam>
    public IList<TStruct> QueryStructs<TStruct>(SqlCmd cmd, string? database = null, string? table = null)
        where TStruct : struct
    {
        var layout = RowLayout.CreateTyped(typeof(TStruct));
        return QueryStructs<TStruct>(cmd, layout, database, table);
    }

    /// <summary>Queries for all matching datasets.</summary>
    /// <param name="cmd">The database dependent sql statement.</param>
    /// <param name="layout">The expected schema layout.</param>
    /// <param name="database">The databasename (optional, used with cached connections).</param>
    /// <param name="table">The tablename (optional, used with cached connections).</param>
    /// <returns>The result rows.</returns>
    /// <typeparam name="TStruct">Result row type.</typeparam>
    public IList<TStruct> QueryStructs<TStruct>(SqlCmd cmd, RowLayout? layout, string? database = null, string? table = null)
        where TStruct : struct
    {
        table ??= layout?.Name;
        var rows = Query(cmd, ref layout, database, table);
        if (layout is null) throw new InvalidOperationException("Layout not given and query could not create it!");
        return rows.Select(r => r.GetStruct<TStruct>(layout)).ToList();
    }

    /// <summary>Querys a single value with a database dependent sql statement.</summary>
    /// <param name="cmd">The database dependent sql statement.</param>
    /// <param name="value">The value read from database.</param>
    /// <param name="database">The affected database (optional, used with cached connections).</param>
    /// <param name="table">The affected table (optional, used with cached connections).</param>
    /// <param name="fieldName">Name of the field (optional, only needed if multiple columns are returned).</param>
    /// <returns>The result value or null.</returns>
    /// <typeparam name="TValue">Result type.</typeparam>
    public bool QueryValue<TValue>(SqlCmd cmd, out TValue value, string? database = null, string? table = null, string? fieldName = null)
        where TValue : struct
    {
        var result = QueryValue(cmd, database, table, fieldName);
        if (result == null)
        {
            value = default;
            return false;
        }

        value = (TValue)result;
        return true;
    }

    /// <summary>Querys a single value with a database dependent sql statement.</summary>
    /// <param name="cmd">The database dependent sql statement.</param>
    /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
    /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
    /// <param name="fieldName">Name of the field (optional, only needed if multiple columns are returned).</param>
    /// <returns>The result value or null.</returns>
    public virtual object? QueryValue(SqlCmd cmd, string? database = null, string? table = null, string? fieldName = null)
    {
        if (cmd == null)
        {
            throw new ArgumentNullException(nameof(cmd));
        }

        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        for (var i = 1; ; i++)
        {
            var connection = GetConnection(database ?? string.Empty);
            var error = false;
            try
            {
                using var command = CreateCommand(connection, cmd);
                if (LogVerboseMessages)
                {
                    LogQuery(command);
                }

                using var reader = command.ExecuteReader(CommandBehavior.KeyInfo);
                var name = table ?? cmd.Text.GetValidChars(ASCII.Strings.SafeName);

                // read schema
                var layout = ReadSchema(reader, name);

                // load rows
                if (!reader.Read())
                {
                    return null;
                }

                var fieldIndex = 0;
                if (fieldName == null)
                {
                    if (layout.FieldCount != 1)
                    {
                        throw new InvalidDataException(
                            $"Error while reading row data: More than one field returned!\n\tDatabase: {database}\n\tTable: {table}\n\tCommand: {cmd}");
                    }
                }
                else
                {
                    fieldIndex = layout.GetFieldIndex(fieldName, true);
                }

                var result = GetLocalValue(layout[fieldIndex], reader, reader.GetValue(fieldIndex));
                if (reader.Read())
                {
                    throw new InvalidDataException(
                        $"Error while reading row data: Additional data available (expected only one row of data)!\n\tDatabase: {database}\n\tTable: {table}\n\tCommand: {cmd}");
                }

                return result;
            }
            catch (Exception ex)
            {
                error = true;
                if (i > MaxErrorRetries)
                {
                    throw;
                }

                Trace.TraceError("<red>{3}<default> Error during QueryValue(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database,
                    table, i, ex.Message, ex);
            }
            finally
            {
                ReturnConnection(ref connection, error);
            }
        }
    }

    /// <summary>Returns a connection to the connection pool for reuse.</summary>
    /// <param name="connection">The connection to return to the queue.</param>
    /// <param name="close">Force close of the connection.</param>
    public void ReturnConnection(ref SqlConnection? connection, bool close)
    {
        if (connection == null)
        {
            return;
        }

        pool.ReturnConnection(ref connection, close);
    }

    #endregion Public Methods
}
