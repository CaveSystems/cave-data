using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cave.Collections.Generic;

namespace Cave.Data.Sql;

/// <summary>Provides a table implementation for generic sql92 databases.</summary>
public abstract class SqlTable : Table
{
    #region Private Fields

    SqlStorage? storage;

    #endregion Private Fields

    #region Private Methods

    void AppendWhereClause(SqlCommandBuilder commandBuilder, Row row, bool useParameters)
    {
        var key = " WHERE ";
        foreach (var field in Layout.Identifier)
        {
            commandBuilder.Append(key);
            commandBuilder.Append(Storage.EscapeFieldName(field));
            commandBuilder.Append("=");

            var value = Storage.GetDatabaseValue(field, row[field.Index]);
            if (useParameters)
            {
                commandBuilder.CreateAndAddParameter(value);
            }
            else
            {
                commandBuilder.Append(Storage.EscapeFieldValue(field, value));
            }
            key = " AND ";
        }
    }

    int InternalCommit(IEnumerable<Transaction> transactions, bool useParameters)
    {
        var n = 0;
        var complete = false;
        var iterator = transactions.GetEnumerator();
        Task? execute = null;
        while (!complete && iterator.MoveNext())
        {
            var commandBuilder = new SqlCommandBuilder(Storage);
            commandBuilder.AppendLine("START TRANSACTION;");
            var i = 0;
            complete = true;
            do
            {
                var transaction = iterator.Current;
                switch (transaction.Type)
                {
                    case TransactionType.Inserted:
                    {
                        CreateInsert(commandBuilder, transaction.Row, useParameters);
                    }
                    break;

                    case TransactionType.Replaced:
                    {
                        CreateReplace(commandBuilder, transaction.Row, useParameters);
                    }
                    break;

                    case TransactionType.Updated:
                    {
                        CreateUpdate(commandBuilder, transaction.Row, useParameters);
                    }
                    break;

                    case TransactionType.Deleted:
                    {
                        commandBuilder.Append("DELETE FROM ");
                        commandBuilder.Append(FQTN);
                        AppendWhereClause(commandBuilder, transaction.Row, useParameters);
                        commandBuilder.AppendLine(";");
                    }
                    break;

                    default: throw new NotImplementedException();
                }

                if (++i >= Storage.TransactionRowCount)
                {
                    complete = false;
                    break;
                }
            }
            while (iterator.MoveNext());

            commandBuilder.AppendLine("COMMIT;");
            try
            {
                if (execute != null)
                {
                    execute.Wait();
                    IncreaseSequenceNumber();
                    Trace.TraceInformation("{0} transactions committed to {1}.", n, FQTN);
                }

                execute = Task.Factory.StartNew(cmd => Execute((SqlCommandBuilder)cmd!), commandBuilder);
                n += i;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"Error committing transactions to table <red>{FQTN}\n{ex}");
                Trace.TraceInformation("Command: {0}", commandBuilder.Text);
                throw;
            }
        }

        execute?.Wait();
        Trace.TraceInformation("{0} transactions committed to {1}.", n, FQTN);
        IncreaseSequenceNumber();
        return n;
    }

    #endregion Private Methods

    #region Protected Methods

    /// <summary>Creates the insert command.</summary>
    /// <param name="commandBuilder">The command builder.</param>
    /// <param name="row">The row.</param>
    /// <param name="useParameters">Use databaseName parameters instead of escaped command string.</param>
    /// <returns>Returns a value indicating whether auto increment was used or not.</returns>
    protected virtual bool CreateInsert(SqlCommandBuilder commandBuilder, Row row, bool useParameters)
    {
        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        if (commandBuilder == null)
        {
            throw new ArgumentNullException(nameof(commandBuilder));
        }
        var usesAutoIncrement = false;

        commandBuilder.Append("INSERT INTO ");
        commandBuilder.Append(FQTN);
        commandBuilder.Append(" (");
        var parameterBuilder = new StringBuilder();
        var firstCommand = true;
        if (Layout.FieldCount != row.FieldCount)
        {
            throw new ArgumentException("Invalid fieldcount at row.", nameof(row));
        }

        for (var i = 0; i < Layout.FieldCount; i++)
        {
            var field = Layout[i];
            if (field.Flags.HasFlag(FieldFlags.AutoIncrement))
            {
                //allow user to set the value explicitly even if field is autoincrement
                var currentValue = Storage.GetDatabaseValue(field, row[i]);
                var autoIncrement = (currentValue == null) || Equals(currentValue, field.DefaultValue);
                if (autoIncrement)
                {
                    usesAutoIncrement = true;
                    continue;
                }
            }

            if (firstCommand)
            {
                firstCommand = false;
            }
            else
            {
                commandBuilder.Append(", ");
                parameterBuilder.Append(", ");
            }

            commandBuilder.Append(Storage.EscapeFieldName(field));
            var value = Storage.GetDatabaseValue(field, row[i]);
            if (value == null)
            {
                parameterBuilder.Append("NULL");
            }
            else if (!useParameters)
            {
                parameterBuilder.Append(Storage.EscapeFieldValue(Layout[i], value));
            }
            else
            {
                var parameter = commandBuilder.CreateParameter(value);
                parameterBuilder.Append(parameter.Name);
            }
        }

        commandBuilder.Append(") VALUES (");
        commandBuilder.Append(parameterBuilder.ToString());
        commandBuilder.Append(")");
        commandBuilder.AppendLine(";");
        return usesAutoIncrement;
    }

    /// <summary>Gets the command to retrieve the last inserted row.</summary>
    /// <param name="commandBuilder">The command builder to append to.</param>
    /// <param name="row">The row to retrieve.</param>
    protected abstract void CreateLastInsertedRowCommand(SqlCommandBuilder commandBuilder, Row row);

    /// <summary>Creates a replace command.</summary>
    /// <param name="commandBuilder">The command builder.</param>
    /// <param name="row">The row.</param>
    /// <param name="useParameters">Use databaseName parameters instead of escaped command string.</param>
    protected virtual void CreateReplace(SqlCommandBuilder commandBuilder, Row row, bool useParameters)
    {
        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        if (commandBuilder == null)
        {
            throw new ArgumentNullException(nameof(commandBuilder));
        }

        commandBuilder.Append("REPLACE INTO ");
        commandBuilder.Append(FQTN);
        commandBuilder.Append(" VALUES (");
        for (var i = 0; i < Layout.FieldCount; i++)
        {
            if (i > 0)
            {
                commandBuilder.Append(",");
            }

            var value = row[i];
            if (value == null)
            {
                commandBuilder.Append("NULL");
            }
            else
            {
                value = Storage.GetDatabaseValue(Layout[i], value);
                if (useParameters)
                {
                    commandBuilder.CreateAndAddParameter(value);
                }
                else
                {
                    commandBuilder.Append(Storage.EscapeFieldValue(Layout[i], value));
                }
            }
        }

        commandBuilder.AppendLine(");");
    }

    /// <summary>Creates an update command.</summary>
    /// <param name="commandBuilder">The command builder.</param>
    /// <param name="row">The row.</param>
    /// <param name="useParameters">Use databaseName parameters instead of escaped command string.</param>
    protected virtual void CreateUpdate(SqlCommandBuilder commandBuilder, Row row, bool useParameters)
    {
        if (commandBuilder == null)
        {
            throw new ArgumentNullException(nameof(commandBuilder));
        }

        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        commandBuilder.Append("UPDATE ");
        commandBuilder.Append(FQTN);
        commandBuilder.Append(" SET ");
        var firstCommand = true;
        for (var i = 0; i < Layout.FieldCount; i++)
        {
            var field = Layout[i];
            if (field.Flags.HasFlag(FieldFlags.ID))
            {
                continue;
            }

            if (firstCommand)
            {
                firstCommand = false;
            }
            else
            {
                commandBuilder.Append(",");
            }

            commandBuilder.Append(Storage.EscapeFieldName(field));
            var value = row[i];
            if (value == null)
            {
                commandBuilder.Append("=NULL");
            }
            else
            {
                commandBuilder.Append("=");
                value = Storage.GetDatabaseValue(Layout[i], value);
                if (useParameters)
                {
                    commandBuilder.CreateAndAddParameter(value);
                }
                else
                {
                    commandBuilder.Append(Storage.EscapeFieldValue(Layout[i], value));
                }
            }
        }

        AppendWhereClause(commandBuilder, row, useParameters);
        commandBuilder.AppendLine(";");
    }

    /// <summary>Retrieves the full layout information for this table.</summary>
    /// <param name="database">Database name.</param>
    /// <param name="table">Table name.</param>
    /// <returns>Returns a new <see cref="RowLayout"/> instance.</returns>
    protected virtual RowLayout QueryLayout(string database, string table) => Storage.QuerySchema(database, table);

    /// <summary>Converts the specified search to a <see cref="SqlSearch"/>.</summary>
    /// <param name="search">Search definition.</param>
    /// <returns>Returns a new <see cref="SqlSearch"/> instance.</returns>
    protected SqlSearch ToSqlSearch(Search search) => new(this, search);

    #endregion Protected Methods

    #region Protected Internal Methods

    /// <summary>Searches the table for rows with given fieldName value combinations.</summary>
    /// <param name="fieldList">List of escaped database field names or *</param>
    /// <param name="search">The search to run.</param>
    /// <param name="opt">Options for the search and the result set.</param>
    /// <returns>Returns number of rows found.</returns>
    protected internal virtual long SqlCount(string fieldList, SqlSearch search, ResultOption opt)
    {
        if (search is null)
        {
            throw new ArgumentNullException(nameof(search));
        }

        if (opt is null)
        {
            throw new ArgumentNullException(nameof(opt));
        }

        if (opt.Contains(ResultOptionMode.Group))
        {
            return SqlCountGroupBy(fieldList, search, opt);
        }

        var command = new StringBuilder();
        command.Append("SELECT COUNT(");
        command.Append(fieldList);
        command.Append(") FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(search);
        foreach (var o in opt.ToArray())
        {
            switch (o.Mode)
            {
                case ResultOptionMode.SortAsc:
                case ResultOptionMode.SortDesc:
                case ResultOptionMode.None:
                    break;

                default:
                    throw new InvalidOperationException($"ResultOptionMode {o.Mode} not supported!");
            }
        }

        var value = Storage.QueryValue(new SqlCmd(command.ToString(), [.. search.Parameters])) ?? throw new InvalidDataException($"Could not read value from {FQTN}!");
        return Convert.ToInt64(value, Storage.Culture);
    }

    /// <summary>Searches for grouped data-sets and returns the number of items found.</summary>
    /// <param name="fieldList">List of escaped database field names or *</param>
    /// <param name="search">Search definition.</param>
    /// <param name="opt">Options for the search.</param>
    /// <returns>Number of items found.</returns>
    protected internal virtual long SqlCountGroupBy(string fieldList, SqlSearch search, ResultOption opt)
    {
        if (search is null)
        {
            throw new ArgumentNullException(nameof(search));
        }

        if (opt is null)
        {
            throw new ArgumentNullException(nameof(opt));
        }

        var command = new StringBuilder();
        command.Append("SELECT COUNT(");
        if (Storage.SupportsAllFieldsGroupBy)
        {
            command.Append(fieldList);
        }
        else
        {
            var fieldNumber = 0;
            foreach (var fieldName in search.FieldNames)
            {
                if (fieldNumber++ > 0)
                {
                    command.Append(", ");
                }

                command.Append(Storage.EscapeFieldName(Layout[fieldName]));
            }
        }

        command.Append(") FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(search);
        if (opt.Contains(ResultOptionMode.Limit) | opt.Contains(ResultOptionMode.Offset))
        {
            throw new InvalidOperationException("Cannot use Option.Group and Option.Limit/Offset at once!");
        }

        var groupCount = 0;
        foreach (var o in opt.Filter(ResultOptionMode.Group))
        {
            if (groupCount++ == 0)
            {
                command.Append(" GROUP BY ");
            }
            else
            {
                command.Append(',');
            }
            if (o.Parameter is null) throw new NullReferenceException($"Parameter may not be null! Option: {opt}");
            command.Append(Storage.EscapeFieldName(Layout[o.Parameter]));
        }

        var value = QueryValue(new SqlCmd(command.ToString(), [.. search.Parameters])) ?? throw new InvalidDataException($"Could not read value from {FQTN}!");
        return Convert.ToInt64(value, null);
    }

    /// <summary>Searches for grouped datasets and returns the id of the first occurence (sql handles this differently).</summary>
    /// <param name="fieldList">List of escaped database field names or *</param>
    /// <param name="search">Search definition.</param>
    /// <param name="opt">Options for the search.</param>
    /// <returns>Returns a list of rows matching the specified criteria.</returns>
    protected internal virtual IList<Row> SqlGetGroupRows(string fieldList, SqlSearch? search, ResultOption? opt)
    {
        if (search is null)
        {
            throw new ArgumentNullException(nameof(search));
        }

        if (opt is null)
        {
            throw new ArgumentNullException(nameof(opt));
        }

        RowLayout? layout;
        var command = new StringBuilder();
        command.Append("SELECT ");
        if (Storage.SupportsAllFieldsGroupBy)
        {
            layout = Layout;
            command.Append(fieldList);
        }
        else
        {
            layout = null;
            var fieldNumber = 0;
            foreach (var fieldName in search.FieldNames)
            {
                if (fieldNumber++ > 0)
                {
                    command.Append(", ");
                }

                command.Append(Storage.EscapeFieldName(Layout[fieldName]));
            }
        }

        command.Append(" FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(search);
        var groupCount = 0;
        foreach (var o in opt.Filter(ResultOptionMode.Group))
        {
            if (groupCount++ == 0)
            {
                command.Append(" GROUP BY ");
            }
            else
            {
                command.Append(',');
            }
            if (o.Parameter is null) throw new NullReferenceException($"Parameter may not be null! Option: {opt}");
            command.Append(Storage.EscapeFieldName(Layout[o.Parameter]));
        }

        return Query(new SqlCmd(command.ToString(), [.. search.Parameters]), ref layout);
    }


    /// <summary>Searches the table for rows with given fieldName value combinations.</summary>
    /// <param name="fieldList">List of escaped database field names or *</param>
    /// <param name="search">The search to run.</param>
    /// <param name="opt">Options for the search and the result set.</param>
    /// <returns>Returns the ID of the row found or -1.</returns>
    protected internal IList<Row> SqlGetRows(string fieldList, SqlSearch search, ResultOption opt)
    {
        var layout = Layout;
        return SqlGetRows(fieldList, ref layout, search, opt);
    }

    /// <summary>Searches the table for rows with given fieldName value combinations.</summary>
    /// <param name="fieldList">List of escaped database field names or *</param>
    /// <param name="layout">Layout to use. If unset the database layout will be used and returned.</param>
    /// <param name="search">The search to run.</param>
    /// <param name="opt">Options for the search and the result set.</param>
    /// <returns>Returns the ID of the row found or -1.</returns>
    protected internal virtual IList<Row> SqlGetRows(string fieldList, ref RowLayout? layout, SqlSearch search, ResultOption opt)
    {
        if (search is null)
        {
            throw new ArgumentNullException(nameof(search));
        }

        if (opt is null)
        {
            throw new ArgumentNullException(nameof(opt));
        }

        if (opt.Contains(ResultOptionMode.Group))
        {
            return SqlGetGroupRows(fieldList, search, opt);
        }

        var command = new StringBuilder();
        command.Append("SELECT ");
        command.Append(fieldList);
        command.Append("FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(search);
        var orderCount = 0;
        foreach (var o in opt.Filter(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
        {
            if (orderCount++ == 0)
            {
                command.Append(" ORDER BY ");
            }
            else
            {
                command.Append(',');
            }

            if (o.Parameter is null) throw new NullReferenceException($"Parameter may not be null! Option: {opt}");
            command.Append(Storage.EscapeFieldName(Layout[o.Parameter]));
            if (o.Mode == ResultOptionMode.SortAsc)
            {
                command.Append(" ASC");
            }
            else
            {
                command.Append(" DESC");
            }
        }

        var limit = 0;
        foreach (var o in opt.Filter(ResultOptionMode.Limit))
        {
            if (limit++ > 0)
            {
                throw new InvalidOperationException("Cannot set two different limits!");
            }

            command.Append(" LIMIT " + o.Parameter);
        }

        var offset = 0;
        foreach (var o in opt.Filter(ResultOptionMode.Offset))
        {
            if (offset++ > 0)
            {
                throw new InvalidOperationException("Cannot set two different offsets!");
            }

            command.Append(" OFFSET " + o.Parameter);
        }

        return Query(new SqlCmd(command.ToString(), [.. search.Parameters]), ref layout);
    }

    #endregion Protected Internal Methods

    #region Public Properties

    /// <summary>Gets the full qualified table name.</summary>
    public string FQTN { get; private set; } = string.Empty;

    /// <inheritdoc/>
    public override long RowCount
    {
        get
        {
            var value = Storage.QueryValue(database: Database.Name, table: Name, cmd: "SELECT COUNT(*) FROM " + FQTN);
            return value == null ? -1 : Convert.ToInt64(value, Storage.Culture);
        }
    }

    /// <summary>Gets or sets the used <see cref="SqlStorage"/> backend.</summary>
    public new SqlStorage Storage => storage ?? (SqlStorage)base.Storage;

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override void Clear()
    {
        Storage.Execute(database: Database.Name, table: Name, cmd: "DELETE FROM " + FQTN);
        IncreaseSequenceNumber();
    }

    /// <inheritdoc/>
    public override int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = default)
    {
        if (transactions == null)
        {
            if (flags.HasFlag(TransactionFlags.NoExceptions)) { return -1; }

            throw new ArgumentNullException(nameof(transactions));
        }

        try
        {
            return InternalCommit(transactions, true);
        }
        catch
        {
            if (!flags.HasFlag(TransactionFlags.NoExceptions))
            {
                throw;
            }

            return -1;
        }
    }

    /// <summary>Initializes the interface class. This is the first method to call after create.</summary>
    /// <remarks>Use this to connect to tables with unknown layout.</remarks>
    /// <param name="database">Database the table belongs to.</param>
    /// <param name="flags">Flags used to connect to the table.</param>
    /// <param name="tableName">Table name to load.</param>
    public void Connect(IDatabase database, TableFlags flags, string tableName)
    {
        if (database is null || database.Storage is not SqlStorage sqlStorage)
        {
            throw new InvalidOperationException("Database has to be a SqlDatabase!");
        }
        storage = sqlStorage;
        FQTN = storage.FQTN(database.Name, tableName);
        var schema = QueryLayout(database.Name, tableName);
        base.Connect(database, flags, schema);
    }

    /// <inheritdoc/>
    public override void Connect(IDatabase database, TableFlags flags, RowLayout layout)
    {
        if (database == null)
        {
            throw new ArgumentNullException(nameof(database));
        }

        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        storage = (database.Storage as SqlStorage) ?? throw new InvalidOperationException("Database has to be a SqlDatabase!");
        FQTN = Storage.FQTN(database.Name, layout.Name);
        var schema = QueryLayout(database.Name, layout.Name);
        Storage.CheckLayout(schema, layout, flags);
        base.Connect(database, flags, schema);
    }

    /// <inheritdoc/>
    public override long Count(Search? search = null, ResultOption? resultOption = null)
    {
        search ??= Search.None;
        resultOption ??= ResultOption.None;
        var sqlSearch = ToSqlSearch(search);
        if (resultOption != ResultOption.None)
        {
            return SqlCount("*", sqlSearch, resultOption);
        }

        var value = QueryValue(new SqlCmd("SELECT COUNT(*) FROM " + FQTN + " WHERE " + sqlSearch, [.. sqlSearch.Parameters])) ?? throw new InvalidDataException($"Could not read row count from {FQTN}!");
        return Convert.ToInt64(value, Storage.Culture);
    }

    /// <inheritdoc/>
    public override void Delete(Row row)
    {
        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        var commandBuilder = new SqlCommandBuilder(Storage);
        commandBuilder.Append("DELETE FROM ");
        commandBuilder.Append(FQTN);
        AppendWhereClause(commandBuilder, row, true);
        Storage.Execute(database: Database.Name, table: Name, cmd: commandBuilder);
        IncreaseSequenceNumber();
    }

    /// <inheritdoc/>
    public override IList<TValue> Distinct<TValue>(string fieldName, Search? search = null)
    {
        var escapedFieldName = Storage.EscapeFieldName(Layout[fieldName]);
        var field = new FieldProperties
        {
            Name = fieldName,
            NameAtDatabase = fieldName,
            Flags = FieldFlags.None,
            DataType = DataType.String,
            TypeAtDatabase = DataType.String
        };
        field.Validate();
        string query;
        if (search == null)
        {
            query = $"SELECT DISTINCT {escapedFieldName} FROM {FQTN}";
        }
        else
        {
            var s = ToSqlSearch(search);
            query = $"SELECT DISTINCT {escapedFieldName} FROM {FQTN} WHERE {s}";
        }

        var rows = Storage.QueryUnchecked(query, Database.Name, Name);
        var result = new Set<TValue>();
        foreach (var row in rows)
        {
            var value = (TValue?)Fields.ConvertValue(typeof(TValue), row[0], CultureInfo.InvariantCulture);
            if (value is not null) result.Include(value);
        }

        return result.AsList();
    }

    /// <summary>Executes a databaseName dependent sql statement silently.</summary>
    /// <param name="cmd">the databaseName dependent sql statement.</param>
    /// <returns>Number of affected rows (if supported by the databaseName).</returns>
    public int Execute(SqlCmd cmd) => Storage.Execute(cmd, Database.Name, Name);

    /// <inheritdoc/>
    public override bool Exist(Search? search)
    {
        search ??= Search.None;
        var s = ToSqlSearch(search);
        var query = "SELECT DISTINCT 1 FROM " + FQTN + " WHERE " + s;
        RowLayout? layout = null;
        return Query(new SqlCmd(query, [.. s.Parameters]), ref layout).Count > 0;
    }

    /// <inheritdoc/>
    public override bool Exist(Row row)
    {
        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        var search = Search.None;
        var i = 0;
        foreach (var field in Layout.Identifier)
        {
            i++;
            search &= Search.FieldEquals(field.Name, row[field.Index]);
        }

        if (i < 1)
        {
            throw new InvalidDataException("At least one identifier fieldName needed!");
        }

        return Exist(search);
    }

    /// <inheritdoc/>
    public override Row GetRow(Search? search = null, ResultOption? resultOption = null) => GetRows(search, resultOption).Single();

    /// <inheritdoc/>
    public override Row GetRowAt(int index) => GetRow(Search.None, ResultOption.Limit(1) + ResultOption.Offset(index));

    /// <inheritdoc/>
    public override IList<Row> GetRows() => Query("SELECT * FROM " + FQTN);

    /// <inheritdoc/>
    public override IList<Row> GetRows(Search? search = null, ResultOption? resultOption = null)
    {
        search ??= Search.None;
        resultOption ??= ResultOption.None;
        var s = ToSqlSearch(search);
        s.CheckFieldsPresent(resultOption);
        return SqlGetRows("*", s, resultOption);
    }

    /// <inheritdoc/>
    public override IList<TValue> GetValues<TValue>(string fieldName, Search? search = null, ResultOption? resultOption = null)
    {
        var escapedFieldName = Storage.EscapeFieldName(Layout[fieldName]);
        var field = new FieldProperties
        {
            Name = fieldName,
            NameAtDatabase = fieldName,
            Flags = FieldFlags.None,
            DataType = DataType.String,
            TypeAtDatabase = DataType.String
        };
        field.Validate();

        search ??= Search.None;
        resultOption ??= ResultOption.None;
        var s = ToSqlSearch(search);
        s.CheckFieldsPresent(resultOption);
        RowLayout? layout = null;
        var rows = SqlGetRows(escapedFieldName, ref layout, s, resultOption);
        var result = new List<TValue>(rows.Count);
        foreach (var row in rows)
        {
            var dbValue = Fields.ConvertValue(typeof(TValue), row[0], Storage.Culture);
            if (dbValue is TValue value) result.Add(value);
        }
        return result;
    }

    /// <inheritdoc/>
    public override Row Insert(Row row)
    {
        var commandBuilder = new SqlCommandBuilder(Storage);
        var autoIncrement = CreateInsert(commandBuilder, row, true);
        Row result;
        if (autoIncrement)
        {
            CreateLastInsertedRowCommand(commandBuilder, row);
            result = QueryRow(commandBuilder);
        }
        else
        {
            Execute(commandBuilder);
            result = GetRow(Search.IdentifierMatch(row));
        }
        IncreaseSequenceNumber();
        return result;
    }

    /// <inheritdoc/>
    public override TValue? Maximum<TValue>(string fieldName, Search? search = null)
    {
        search ??= Search.None;

        var command = new SqlCommandBuilder(Storage);
        command.Append("SELECT MAX(");
        command.Append(Storage.EscapeFieldName(Layout[fieldName]));
        command.Append(") FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(ToSqlSearch(search).ToString());
        var value = Storage.QueryValue(database: Database.Name, table: Name, cmd: command);
        return value == null ? null : (TValue)value;
    }

    /// <inheritdoc/>
    public override TValue? Minimum<TValue>(string fieldName, Search? search = null)
    {
        search ??= Search.None;

        var command = new SqlCommandBuilder(Storage);
        command.Append("SELECT MIN(");
        command.Append(Storage.EscapeFieldName(Layout[fieldName]));
        command.Append(") FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(ToSqlSearch(search).ToString());
        var value = Storage.QueryValue(database: Database.Name, table: Name, cmd: command);
        return value == null ? null : (TValue)value;
    }

    /// <summary>Queries for all matching datasets.</summary>
    /// <param name="cmd">The databaseName dependent sql statement.</param>
    /// <param name="layout">The expected schema layout (if unset the layout is returned).</param>
    /// <returns>The result rows.</returns>
    public IList<Row> Query(SqlCmd cmd, ref RowLayout? layout) => Storage.QueryChecked(cmd, ref layout, Database.Name, Name);

    /// <summary>Queries for all matching datasets.</summary>
    /// <param name="cmd">The databaseName dependent sql statement.</param>
    /// <returns>The result rows.</returns>
    public IList<Row> Query(SqlCmd cmd)
    {
        var layout = Layout;
        return Query(cmd, ref layout);
    }

    /// <summary>Queries for a dataset (selected fields, one row).</summary>
    /// <param name="cmd">The databaseName dependent sql statement.</param>
    /// <param name="layout">The expected schema layout (if unset the layout is returned).</param>
    /// <returns>The result row.</returns>
    public Row QueryRow(SqlCmd cmd, ref RowLayout? layout) => Query(cmd, ref layout).Single();

    /// <summary>Queries for a dataset (selected fields, one row).</summary>
    /// <param name="cmd">The databaseName dependent sql statement.</param>
    /// <returns>The result row.</returns>
    public Row QueryRow(SqlCmd cmd)
    {
        var layout = Layout;
        return QueryRow(cmd, ref layout);
    }

    /// <summary>Querys a single value with a databaseName dependent sql statement.</summary>
    /// <param name="cmd">The databaseName dependent sql statement.</param>
    /// <param name="value">The result.</param>
    /// <param name="fieldName">Name of the fieldName (optional, only needed if multiple columns are returned).</param>
    /// <returns>true if the value could be found and read, false otherwise.</returns>
    /// <typeparam name="TValue">Result value type.</typeparam>
    public bool QueryValue<TValue>(SqlCmd cmd, out TValue value, string? fieldName = null)
        where TValue : struct =>
        Storage.QueryValue(cmd, out value, Database.Name, Name, fieldName);

    /// <summary>Querys a single value with a databaseName dependent sql statement.</summary>
    /// <param name="cmd">The databaseName dependent sql statement.</param>
    /// <param name="fieldName">Name of the fieldName (optional, only needed if multiple columns are returned).</param>
    /// <returns>The result value or null.</returns>
    public object? QueryValue(SqlCmd cmd, string? fieldName = null) => Storage.QueryValue(cmd, Database.Name, Name, fieldName);

    /// <inheritdoc/>
    public override void Replace(Row row)
    {
        var commandBuilder = new SqlCommandBuilder(Storage);
        CreateReplace(commandBuilder, row, true);
        Execute(commandBuilder);
        IncreaseSequenceNumber();
    }

    /// <inheritdoc/>
    public override void SetValue(string fieldName, object value)
    {
        var field = Layout[fieldName];
        var command = "UPDATE " + FQTN + " SET " + Storage.EscapeFieldName(field);
        if (value == null)
        {
            Storage.Execute(command + "=NULL");
        }
        else
        {
            var parameter = new SqlParam(Storage.ParameterPrefix, value);
            Execute(new SqlCmd($"{command}={parameter.Name};", parameter));
        }

        IncreaseSequenceNumber();
    }

    /// <inheritdoc/>
    public override double Sum(string fieldName, Search? search = null)
    {
        var field = Layout[fieldName];
        search ??= Search.None;

        var s = ToSqlSearch(search);
        var command = new StringBuilder();
        command.Append("SELECT SUM(");
        command.Append(Storage.EscapeFieldName(field));
        command.Append(") FROM ");
        command.Append(FQTN);
        command.Append(" WHERE ");
        command.Append(s);
        var result = double.NaN;
        var value = Storage.QueryValue(new SqlCmd(command.ToString(), [.. s.Parameters])) ?? throw new InvalidDataException($"Could not read value from {FQTN}!");
        switch (field.DataType)
        {
            case DataType.Binary:
            case DataType.DateTime:
            case DataType.String:
            case DataType.User:
            case DataType.Unknown:
                throw new NotSupportedException($"Sum() is not supported for fieldName {field}!");
            case DataType.TimeSpan:
                switch (field.DateTimeType)
                {
                    case DateTimeType.BigIntHumanReadable:
                    case DateTimeType.Undefined:
                        throw new NotSupportedException($"Sum() is not supported for fieldName {field}!");
                    case DateTimeType.BigIntTicks:
                        result = Convert.ToDouble(value, CultureInfo.CurrentCulture) / TimeSpan.TicksPerSecond;
                        break;

                    case DateTimeType.BigIntSeconds:
                        result = Convert.ToDouble(value, CultureInfo.CurrentCulture);
                        break;

                    case DateTimeType.BigIntMilliSeconds:
                        result = Convert.ToDouble(value, CultureInfo.CurrentCulture) / TimeSpan.TicksPerMillisecond;
                        break;

                    case DateTimeType.DecimalSeconds:
                    case DateTimeType.Native:
                    case DateTimeType.DoubleSeconds:
                        result = Convert.ToDouble(value, CultureInfo.CurrentCulture);
                        break;
                }

                break;

            default:
                result = Convert.ToDouble(value, CultureInfo.CurrentCulture);
                break;
        }

        return result;
    }

    /// <summary>Gets the name of the table.</summary>
    /// <returns>Database.Tablename.</returns>
    public override string ToString() => Storage.FQTN(Database.Name, Name);

    /// <inheritdoc/>
    public override int TryDelete(Search? search)
    {
        search ??= Search.None;
        var s = ToSqlSearch(search);
        var command = "DELETE FROM " + FQTN + " WHERE " + s;
        var result = Execute(new SqlCmd(command, [.. s.Parameters]));
        IncreaseSequenceNumber();
        return result;
    }

    /// <inheritdoc/>
    public override void Update(Row row)
    {
        var commandBuilder = new SqlCommandBuilder(Storage);
        CreateUpdate(commandBuilder, row, true);
        Execute(commandBuilder);
        IncreaseSequenceNumber();
    }

    #endregion Public Methods
}
