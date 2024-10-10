using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Sqlite;

/// <summary>Provides a sqlite databaseName implementation.</summary>
public sealed class SqliteDatabase : SqlDatabase
{
    #region Protected Methods

    /// <inheritdoc/>
    protected override string[] GetTableNames()
    {
        var result = new List<string>();
        var rows = SqlStorage.Query(database: Name, table: "sqlite_master",
            cmd: "SELECT name, type FROM sqlite_master WHERE type='tableName' AND name NOT LIKE 'sqlite_%'");
        foreach (var row in rows)
        {
            result.Add(row[0]?.ToString() ?? throw new InvalidOperationException("information_schema did not return table name!"));
        }

        return [.. result];
    }

    #endregion Protected Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="SqliteDatabase"/> class.</summary>
    /// <param name="storage">The storage engine.</param>
    /// <param name="name">The name of the databaseName.</param>
    public SqliteDatabase(SqliteStorage storage, string name)
        : base(storage, name)
    {
        var fields = new List<FieldProperties>
        {
            new() { Index = 0, DataType = DataType.String, Name = "type" },
            new() { Index = 1, DataType = DataType.String, Name = "name" },
            new() { Index = 2, DataType = DataType.String, Name = "tbname" },
            new() { Index = 3, DataType = DataType.Int64, Name = "rootpage" },
            new() { Index = 4, DataType = DataType.String, Name = "sql" }
        };
        foreach (var field in fields)
        {
            field.NameAtDatabase = field.Name;
            field.TypeAtDatabase = field.DataType;
            field.Validate();
        }

        var table = RowLayout.CreateUntyped(name, fields.ToArray());
        var schema = SqlStorage.QuerySchema(Name, "sqlite_master");
        SqlStorage.CheckLayout(schema, table, 0);
    }

    #endregion Public Constructors

    #region Public Properties

    /// <inheritdoc/>
    public override bool IsSecure => true;

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override ITable CreateTable(RowLayout layout, TableFlags flags)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        if ((flags & TableFlags.InMemory) != 0)
        {
            throw new NotSupportedException($"Table '{layout.Name}' does not support TableFlags.{flags}");
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

            queryText.Append(fieldProperties.NameAtDatabase);
            queryText.Append(' ');
            var valueType = SqliteStorage.GetValueType(fieldProperties.DataType);
            queryText.Append(valueType.ToString());
            if ((fieldProperties.Flags & FieldFlags.ID) != 0)
            {
                queryText.Append(" PRIMARY KEY");
            }

            if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
            {
                queryText.Append(" AUTOINCREMENT");
            }

            if ((fieldProperties.Flags & FieldFlags.Unique) != 0)
            {
                queryText.Append(" UNIQUE");
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
                var command = $"CREATE INDEX {"idx_" + layout.Name + "_" + fieldProperties.Name} ON {layout.Name} ({fieldProperties.Name})";
                SqlStorage.Execute(database: Name, table: layout.Name, cmd: command);
            }
        }

        return GetTable(layout);
    }

    /// <inheritdoc/>
    public override ITable GetTable(string tableName, TableFlags flags) => SqliteTable.Connect(this, flags, tableName);

    #endregion Public Methods
}
