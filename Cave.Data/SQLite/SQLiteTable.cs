using System;
using System.Linq;
using Cave.Data.Sql;

namespace Cave.Data.Sqlite;

/// <summary>Provides a sqlite table implementation.</summary>
public class SqliteTable : SqlTable
{
    #region Protected Constructors

    /// <summary>Initializes a new instance of the <see cref="SqliteTable"/> class.</summary>
    protected SqliteTable() { }

    #endregion Protected Constructors

    #region Protected Methods

    /// <inheritdoc/>
    protected override void CreateLastInsertedRowCommand(SqlCommandBuilder commandBuilder, Row row)
    {
        if (commandBuilder == null)
        {
            throw new ArgumentNullException(nameof(commandBuilder));
        }

        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        var idField = Layout.Identifier.Single();
        commandBuilder.AppendLine($"SELECT * FROM {FQTN} WHERE {Storage.EscapeFieldName(idField)} = last_insert_rowid();");
    }

    #endregion Protected Methods

    #region Public Methods

    /// <summary>Connects to the specified database and tablename.</summary>
    /// <param name="database">Database to connect to.</param>
    /// <param name="flags">Flags used to connect to the table.</param>
    /// <param name="tableName">The table to connect to.</param>
    /// <returns>Returns a new <see cref="SqliteTable"/> instance.</returns>
    public static SqliteTable Connect(SqliteDatabase database, TableFlags flags, string tableName)
    {
        var table = new SqliteTable();
        table.Initialize(database, flags, tableName);
        return table;
    }

    /// <inheritdoc/>
    public override Row GetRowAt(int index) => QueryRow($"SELECT * FROM {FQTN} LIMIT {index},1");

    #endregion Public Methods
}
