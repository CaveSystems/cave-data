using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;
using Cave.IO;

namespace Cave.Data.Mssql;

/// <summary>Provides a MsSql database implementation.</summary>
public sealed class MssqlDatabase : SqlDatabase
{
    #region Protected Methods

    /// <inheritdoc/>
    protected override string[] GetTableNames()
    {
        var result = new List<string>();
        var rows = SqlStorage.Query("EXEC stables @table_owner='dbo',@table_qualifier='" + Name + "';");
        foreach (var row in rows)
        {
            var tableName = row[2]?.ToString();
            if (tableName is not null) result.Add(tableName);
        }

        return [.. result];
    }

    #endregion Protected Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="MssqlDatabase"/> class.</summary>
    /// <param name="storage">the MsSql storage engine.</param>
    /// <param name="name">the name of the database.</param>
    public MssqlDatabase(MssqlStorage storage, string name)
        : base(storage, name)
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
                var value = SqlStorage.QueryValue("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID");
                return bool.Parse(value?.ToString() ?? "false");
            }
            catch (Exception ex)
            {
                error = true;
                throw new NotSupportedException("Could not retrieve connection state.", ex);
            }
            finally
            {
                SqlStorage.ReturnConnection(ref connection, error);
            }
        }
    }

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override ITable CreateTable(RowLayout layout, TableFlags flags)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        LogCreateTable(layout);
        if ((flags & TableFlags.InMemory) != 0)
        {
            throw new NotSupportedException($"Table '{layout.Name}' does not support TableFlags.{TableFlags.InMemory}");
        }

        if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException($"Table name {layout.Name} contains invalid chars!");
        }

        var queryText = new StringBuilder();
        queryText.Append($"CREATE TABLE {SqlStorage.FQTN(Name, layout.Name)} (");
        for (var i = 0; i < layout.FieldCount; i++)
        {
            var fieldProperties = layout[i];
            if (i > 0)
            {
                queryText.Append(',');
            }

            queryText.Append(fieldProperties.NameAtDatabase + " ");
            switch (fieldProperties.DataType)
            {
                case DataType.Binary:
                    queryText.Append("VARBINARY(MAX)");
                    break;

                case DataType.Bool:
                    queryText.Append("BIT");
                    break;

                case DataType.DateTime:
                    switch (fieldProperties.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            queryText.Append("DATETIME");
                            break;

                        case DateTimeType.DoubleSeconds:
                        case DateTimeType.DoubleEpoch:
                            queryText.Append("FLOAT(53)");
                            break;

                        case DateTimeType.DecimalSeconds:
                            queryText.Append("NUMERIC(28,8)");
                            break;

                        case DateTimeType.BigIntHumanReadable:
                        case DateTimeType.BigIntTicks:
                            queryText.Append("BIGINT");
                            break;

                        default: throw new NotImplementedException();
                    }

                    break;

                case DataType.TimeSpan:
                    switch (fieldProperties.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            queryText.Append("TIMESPAN");
                            break;

                        case DateTimeType.DoubleEpoch:
                        case DateTimeType.DoubleSeconds:
                            queryText.Append("FLOAT(53)");
                            break;

                        case DateTimeType.DecimalSeconds:
                            queryText.Append("NUMERIC(28,8)");
                            break;

                        case DateTimeType.BigIntHumanReadable:
                        case DateTimeType.BigIntTicks:
                            queryText.Append("BIGINT");
                            break;

                        default: throw new NotImplementedException();
                    }

                    break;

                case DataType.Int8:
                    queryText.Append("SMALLINT");
                    break;

                case DataType.Int16:
                    queryText.Append("SMALLINT");
                    break;

                case DataType.Int32:
                    queryText.Append("INTEGER");
                    break;

                case DataType.Int64:
                    queryText.Append("BIGINT");
                    break;

                case DataType.Single:
                    queryText.Append("REAL");
                    break;

                case DataType.Double:
                    queryText.Append("FLOAT(53)");
                    break;

                case DataType.Enum:
                    queryText.Append("BIGINT");
                    break;

                case DataType.User:
                case DataType.String:
                    switch (fieldProperties.StringEncoding)
                    {
                        case StringEncoding.US_ASCII:
#pragma warning disable CS0618
                        case StringEncoding.ASCII:
#pragma warning restore CS0618
                            if (fieldProperties.MaximumLength is > 0 and <= 255)
                            {
                                queryText.Append($"VARCHAR({fieldProperties.MaximumLength})");
                            }
                            else
                            {
                                queryText.Append("VARCHAR(MAX)");
                            }

                            break;

                        default:
                            if (fieldProperties.MaximumLength is > 0 and <= 255)
                            {
                                queryText.Append($"NVARCHAR({fieldProperties.MaximumLength})");
                            }
                            else
                            {
                                queryText.Append("NVARCHAR(MAX)");
                            }

                            break;
                    }

                    break;

                case DataType.Decimal:
                    if (fieldProperties.MaximumLength > 0)
                    {
                        var precision = (int)fieldProperties.MaximumLength;
                        var scale = (int)((fieldProperties.MaximumLength - precision) * 100);
                        if (scale >= precision)
                        {
                            throw new ArgumentOutOfRangeException(
                                $"Field {fieldProperties.Name} has an invalid MaximumLength of {precision},{scale}. Correct values range from s,p = 1,0 to 28,27 with 0 < s < p!");
                        }

                        queryText.Append($"NUMERIC({precision},{scale})");
                    }
                    else
                    {
                        queryText.Append("NUMERIC(28,8)");
                    }

                    break;

                default: throw new NotImplementedException($"Unknown DataType {fieldProperties.DataType}!");
            }

            if ((fieldProperties.Flags & FieldFlags.ID) != 0)
            {
                queryText.Append(" PRIMARY KEY");
            }

            if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
            {
                queryText.Append(" IDENTITY");
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

                queryText.Append(" COMMENT '" + fieldProperties.Description[..60].Replace('\'', '`') + "'");
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
                var command =
                    $"CREATE INDEX idx_{SqlStorage.EscapeString(fieldProperties.NameAtDatabase)} ON {SqlStorage.FQTN(Name, layout.Name)} ({SqlStorage.EscapeFieldName(fieldProperties)})";
                SqlStorage.Execute(database: Name, table: layout.Name, cmd: command);
            }
        }

        return GetTable(layout);
    }

    /// <inheritdoc/>
    public override ITable GetTable(string tableName, TableFlags flags) => MssqlTable.Connect(this, flags, tableName);

    #endregion Public Methods
}
