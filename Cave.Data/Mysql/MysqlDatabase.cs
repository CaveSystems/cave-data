using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cave.Data.Sql;
using Cave.IO;

namespace Cave.Data.Mysql;

/// <summary>Provides a mysql database implementation.</summary>
public sealed class MysqlDatabase : SqlDatabase
{
    #region Protected Methods

    /// <inheritdoc/>
    protected override string[] GetTableNames()
    {
        var result = new List<string>();
        var rows = SqlStorage.Query(database: "information_schema", table: "TABLES",
            cmd: "SELECT table_name,table_schema,table_type FROM information_schema.TABLES where table_type='BASE TABLE' AND table_schema LIKE " +
            SqlStorage.EscapeString(Name));
        foreach (var row in rows)
        {
            result.Add(row[0]?.ToString() ?? throw new InvalidOperationException("information_schema did not return table name!"));
        }

        return [.. result];
    }

    #endregion Protected Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="MysqlDatabase"/> class.</summary>
    /// <param name="storage">the mysql storage engine.</param>
    /// <param name="name">the name of the database.</param>
    public MysqlDatabase(MysqlStorage storage, string name)
        : base(storage, name)
    {
    }

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets a value indicating whether this instance is using a secure connection to the storage.</summary>
    /// <value><c>true</c> if this instance is using a secure connection; otherwise, <c>false</c>.</value>
    public override bool IsSecure
    {
        get
        {
            var error = false;
            var connection = SqlStorage.GetConnection(Name);
            try
            {
                return connection.ConnectionString.ToUpperInvariant().Contains("SSLMODE=REQUIRED");
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

    /// <inheritdoc/>
    public override ITable CreateTable(RowLayout layout, TableFlags flags)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException($"Table name {layout.Name} contains invalid chars!");
        }

        var utf8Charset = ((MysqlStorage)Storage).CharacterSet;
        LogCreateTable(layout);
        var queryText = new StringBuilder();
        queryText.Append($"CREATE TABLE {SqlStorage.FQTN(Name, layout.Name)} (");
        for (var i = 0; i < layout.FieldCount; i++)
        {
            var fieldProperties = layout[i];
            if (i > 0)
            {
                queryText.Append(',');
            }

            queryText.Append(SqlStorage.EscapeFieldName(fieldProperties));
            queryText.Append(' ');
            switch (fieldProperties.TypeAtDatabase)
            {
                case DataType.Binary:
                    if (fieldProperties.MaximumLength <= 0)
                    {
                        queryText.Append("LONGBLOB");
                    }
                    else if (fieldProperties.MaximumLength <= 255)
                    {
                        queryText.Append("TINYBLOB");
                    }
                    else if (fieldProperties.MaximumLength <= 65535)
                    {
                        queryText.Append("BLOB");
                    }
                    else if (fieldProperties.MaximumLength <= 16777215)
                    {
                        queryText.Append("MEDIUMBLOB");
                    }
                    else
                    {
                        queryText.Append("LONGBLOB");
                    }

                    break;

                case DataType.Bool:
                    queryText.Append("TINYINT(1)");
                    break;

                case DataType.DateTime:
                    switch (fieldProperties.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            queryText.Append("DATETIME");
                            break;

                        case DateTimeType.DoubleEpoch:
                        case DateTimeType.DoubleSeconds:
                            queryText.Append("DOUBLE");
                            break;

                        case DateTimeType.DecimalSeconds:
                            queryText.Append("DECIMAL(65,30)");
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
                            queryText.Append("TIME");
                            break;

                        case DateTimeType.DoubleEpoch:
                        case DateTimeType.DoubleSeconds:
                            queryText.Append("DOUBLE");
                            break;

                        case DateTimeType.DecimalSeconds:
                            queryText.Append("DECIMAL(65,30)");
                            break;

                        case DateTimeType.BigIntHumanReadable:
                        case DateTimeType.BigIntTicks:
                            queryText.Append("BIGINT");
                            break;

                        default: throw new NotImplementedException();
                    }

                    break;

                case DataType.Int8:
                    queryText.Append("TINYINT");
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
                    queryText.Append("FLOAT");
                    break;

                case DataType.Double:
                    queryText.Append("DOUBLE");
                    break;

                case DataType.Enum:
                    queryText.Append("BIGINT");
                    break;

                case DataType.UInt8:
                    queryText.Append("TINYINT UNSIGNED");
                    break;

                case DataType.UInt16:
                    queryText.Append("SMALLINT UNSIGNED");
                    break;

                case DataType.UInt32:
                    queryText.Append("INTEGER UNSIGNED");
                    break;

                case DataType.UInt64:
                    queryText.Append("BIGINT UNSIGNED");
                    break;

                case DataType.User:
                case DataType.String:
                    if (fieldProperties.MaximumLength <= 0)
                    {
                        queryText.Append("LONGTEXT");
                    }
                    else if (fieldProperties.MaximumLength <= 255)
                    {
                        queryText.Append($"VARCHAR({fieldProperties.MaximumLength})");
                    }
                    else if (fieldProperties.MaximumLength <= 65535)
                    {
                        queryText.Append("TEXT");
                    }
                    else if (fieldProperties.MaximumLength <= 16777215)
                    {
                        queryText.Append("MEDIUMTEXT");
                    }
                    else
                    {
                        queryText.Append("LONGTEXT");
                    }

                    switch (fieldProperties.StringEncoding)
                    {
                        case StringEncoding.Undefined:
                        case StringEncoding.US_ASCII:
                        case StringEncoding.ASCII:
                            queryText.Append(" CHARACTER SET latin1 COLLATE latin1_general_ci");
                            break;

                        case StringEncoding.UTF_8:
                            queryText.Append($" CHARACTER SET utf8mb4 COLLATE {utf8Charset}_general_ci");
                            break;

                        case StringEncoding.UTF_16:
                            queryText.Append(" CHARACTER SET ucs2 COLLATE ucs2_general_ci");
                            break;

                        case StringEncoding.UTF_32:
                            queryText.Append(" CHARACTER SET utf32 COLLATE utf32_general_ci");
                            break;

                        default: throw new NotSupportedException($"MYSQL Server does not support {fieldProperties.StringEncoding}!");
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

            if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
            {
                queryText.Append(" AUTO_INCREMENT");
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

        if (layout.Identifier.Any())
        {
            queryText.Append(",PRIMARY KEY(");
            var count = 0;
            foreach (var field in layout.Identifier)
            {
                if (count++ > 0)
                {
                    queryText.Append(',');
                }

                queryText.Append(SqlStorage.EscapeFieldName(field));
            }

            queryText.Append(')');
        }

        queryText.Append(')');
        if ((flags & TableFlags.InMemory) != 0)
        {
            queryText.Append(" ENGINE = MEMORY");
        }

        SqlStorage.Execute(database: Name, table: layout.Name, cmd: queryText.ToString());
        try
        {
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var field = layout[i];
                if ((field.Flags & FieldFlags.ID) != 0)
                {
                    continue;
                }

                if ((field.Flags & FieldFlags.Index) != 0)
                {
                    string command;
                    switch (field.DataType)
                    {
                        case DataType.Binary:
                        case DataType.String:
                        case DataType.User:
                            var size = (int)field.MaximumLength;
                            if (size < 1)
                            {
                                size = 32;
                            }

                            command =
                                $"CREATE INDEX `idx_{layout.Name}_{field.Name}` ON {SqlStorage.FQTN(Name, layout.Name)} ({SqlStorage.EscapeFieldName(field)} ({size}))";
                            break;

                        case DataType.Bool:
                        case DataType.Char:
                        case DataType.DateTime:
                        case DataType.Decimal:
                        case DataType.Double:
                        case DataType.Enum:
                        case DataType.Int16:
                        case DataType.Int32:
                        case DataType.Int64:
                        case DataType.Int8:
                        case DataType.Single:
                        case DataType.TimeSpan:
                        case DataType.UInt16:
                        case DataType.UInt32:
                        case DataType.UInt64:
                        case DataType.UInt8:
                            command =
                                $"CREATE INDEX `idx_{layout.Name}_{field.Name}` ON {SqlStorage.FQTN(Name, layout.Name)} ({SqlStorage.EscapeFieldName(field)})";
                            break;

                        default: throw new NotSupportedException($"INDEX for datatype of field {field} is not supported!");
                    }

                    SqlStorage.Execute(database: Name, table: layout.Name, cmd: command);
                }
            }
        }
        catch
        {
            DeleteTable(layout.Name);
            throw;
        }

        return GetTable(layout);
    }

    /// <inheritdoc/>
    public override ITable GetTable(string tableName, TableFlags flags) => MysqlTable.Connect(this, flags, tableName);

    #endregion Public Methods
}
