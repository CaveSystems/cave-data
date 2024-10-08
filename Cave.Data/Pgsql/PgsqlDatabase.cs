using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Pgsql;

/// <summary>Provides a postgre sql database implementation.</summary>
public sealed class PgsqlDatabase : SqlDatabase
{
    #region Protected Methods

    /// <inheritdoc/>
    protected override string[] GetTableNames() =>
        SqlStorage.Query(database: Name, table: "pg_tables", cmd: "SELECT tablename FROM pg_tables").Select(r => r[0]?.ToString()).Where(s => s is not null).ToArray()!;

    #endregion Protected Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="PgsqlDatabase"/> class.</summary>
    /// <param name="storage">the postgre sql storage engine.</param>
    /// <param name="name">the name of the database.</param>
    public PgsqlDatabase(PgsqlStorage storage, string name)
        : base(storage, PgsqlStorage.GetObjectName(name))
    {
    }

    #endregion Public Constructors

    #region Public Properties

    /// <inheritdoc/>
    public override bool IsSecure
    {
        get
        {
            var error = false;
            var connection = SqlStorage.GetConnection(Name);
            try
            {
                return connection.ConnectionString.ToUpperInvariant().Contains("SSLMODE=REQUIRE");
            }
            catch
            {
                error = true;
                throw;
            }
            finally
            {
                SqlStorage.ReturnConnection(ref connection, error);
            }
        }
    }

    #endregion Public Properties

    #region Public Methods

    /// <summary>Adds a new tableName with the specified name.</summary>
    /// <param name="layout">Layout of the tableName.</param>
    /// <param name="flags">The flags for tableName creation.</param>
    /// <returns>Returns an <see cref="ITable"/> instance for the specified tableName.</returns>
    public override ITable CreateTable(RowLayout layout, TableFlags flags = default)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        Trace.TraceInformation($"Creating tableName {layout}");
        if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException($"Table name {layout.Name} contains invalid chars!");
        }

        var queryText = new StringBuilder();
        queryText.Append("CREATE ");
        if ((flags & TableFlags.InMemory) != 0)
        {
            queryText.Append("UNLOGGED ");
        }

        queryText.Append($"TABLE {SqlStorage.FQTN(Name, layout.Name)} (");
        for (var i = 0; i < layout.FieldCount; i++)
        {
            var fieldProperties = layout[i];
            if (i > 0)
            {
                queryText.Append(',');
            }

            var fieldName = SqlStorage.EscapeFieldName(fieldProperties);
            queryText.Append(fieldName);
            queryText.Append(' ');
            switch (fieldProperties.TypeAtDatabase)
            {
                case DataType.Binary:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("BYTEA");
                    break;

                case DataType.Bool:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("BOOL");
                    break;

                case DataType.DateTime:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("TIMESTAMP WITH TIME ZONE");
                    break;

                case DataType.TimeSpan:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("FLOAT8");
                    break;

                case DataType.Int8:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("SMALLINT");
                    break;

                case DataType.Int16:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        queryText.Append("SMALLSERIAL");
                    }
                    else
                    {
                        queryText.Append("SMALLINT");
                    }

                    break;

                case DataType.Int32:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        queryText.Append("SERIAL");
                    }
                    else
                    {
                        queryText.Append("INTEGER");
                    }

                    break;

                case DataType.Int64:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        queryText.Append("BIGSERIAL");
                    }
                    else
                    {
                        queryText.Append("BIGINT");
                    }

                    break;

                case DataType.Single:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("FLOAT4");
                    break;

                case DataType.Double:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("FLOAT8");
                    break;

                case DataType.Enum:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append("BIGINT");
                    break;

                case DataType.UInt8:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append($"SMALLINT CHECK ({fieldName} >= 0 AND {fieldName} <= {byte.MaxValue})");
                    break;

                case DataType.UInt16:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append($"INT CHECK ({fieldName} >= 0 AND {fieldName} <= {ushort.MaxValue})");
                    break;

                case DataType.UInt32:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append($"BIGINT CHECK ({fieldName} >= 0 AND {fieldName} <= {uint.MaxValue})");
                    break;

                case DataType.UInt64:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    queryText.Append($"NUMERIC(20,0) CHECK ({fieldName} >= 0 AND {fieldName} <= {ulong.MaxValue})");
                    break;

                case DataType.User:
                case DataType.String:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    if (fieldProperties.MaximumLength <= 0)
                    {
                        queryText.Append("TEXT");
                    }
                    else
                    {
                        queryText.Append($"VARCHAR({fieldProperties.MaximumLength})");
                    }

                    break;

                case DataType.Decimal:
                    if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                    {
                        throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                    }

                    if (fieldProperties.MaximumLength > 0)
                    {
                        var precision = (int)fieldProperties.MaximumLength;
                        var scale = (int)((fieldProperties.MaximumLength - precision) * 100);
                        if (scale >= precision)
                        {
                            throw new ArgumentOutOfRangeException(
                                $"Field {fieldProperties.Name} has an invalid MaximumLength of {precision},{scale}. Correct values range from s,p = 1,0 to 65,30(default value) with 0 < s < p!");
                        }

                        queryText.Append($"DECIMAL({precision},{scale})");
                    }
                    else
                    {
                        queryText.Append("DECIMAL(65,30)");
                    }

                    break;

                default: throw new NotImplementedException($"Unknown DataType {fieldProperties.DataType}!");
            }

            if ((fieldProperties.Flags & FieldFlags.ID) != 0)
            {
                queryText.Append(" PRIMARY KEY");
            }

            if ((fieldProperties.Flags & FieldFlags.Unique) != 0)
            {
                queryText.Append(" UNIQUE");
                switch (fieldProperties.TypeAtDatabase)
                {
                    case DataType.Bool:
                    case DataType.Char:
                    case DataType.DateTime:
                    case DataType.Decimal:
                    case DataType.Double:
                    case DataType.Enum:
                    case DataType.Int8:
                    case DataType.Int16:
                    case DataType.Int32:
                    case DataType.Int64:
                    case DataType.UInt8:
                    case DataType.UInt16:
                    case DataType.UInt32:
                    case DataType.UInt64:
                    case DataType.Single:
                    case DataType.TimeSpan:
                        break;

                    case DataType.String:
                        if (fieldProperties.MaximumLength <= 0)
                        {
                            throw new NotSupportedException(
                                $"Unique string fields without length are not supported! Please define Field.MaxLength at tableName {layout.Name} field {fieldProperties.Name}");
                        }

                        break;

                    default: throw new NotSupportedException($"Uniqueness for tableName {layout.Name} field {fieldProperties.Name} is not supported!");
                }
            }

            if (fieldProperties.Description != null)
            {
                if (fieldProperties.Description.HasInvalidChars(ASCII.Strings.Printable))
                {
                    throw new ArgumentException("Description of field '{0}' contains invalid chars!", fieldProperties.Name);
                }

                var description = fieldProperties.Description;
                if (description.Length > 60) description = description[..58] + "..";
                queryText.Append($" COMMENT '{description}'");
            }
        }

        queryText.Append(')');
        SqlStorage.Execute(database: Name, table: layout.Name, cmd: queryText.ToString());
        for (var i = 0; i < layout.FieldCount; i++)
        {
            var fieldProperties = layout[i];
            if ((fieldProperties.Flags & FieldFlags.ID) != 0)
            {
                continue;
            }

            if ((fieldProperties.Flags & FieldFlags.Index) != 0)
            {
                var name = PgsqlStorage.GetObjectName($"idx_{layout.Name}_{fieldProperties.Name}");
                var cmd = $"CREATE INDEX {name} ON {SqlStorage.FQTN(Name, layout.Name)} ({SqlStorage.EscapeFieldName(fieldProperties)})";
                SqlStorage.Execute(database: Name, table: layout.Name, cmd: cmd);
            }
        }

        return GetTable(layout);
    }

    /// <inheritdoc/>
    public override ITable GetTable(string tableName, TableFlags flags = default) => PgsqlTable.Connect(this, flags, tableName);

    #endregion Public Methods
}
