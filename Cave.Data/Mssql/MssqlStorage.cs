using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Mssql;

/// <summary>Provides a MsSql storage implementation.</summary>
public sealed class MssqlStorage : SqlStorage
{
    #region Protected Methods

    /// <inheritdoc/>
    protected override string GetConnectionString(string database)
    {
        var requireSSL = !AllowUnsafeConnections;
        if (requireSSL)
        {
            if (ConnectionString.Server is "127.0.0.1" or "::1" or "localhost")
            {
                requireSSL = false;
            }
        }

        var result = new StringBuilder();
        result.Append("Server=");
        result.Append(ConnectionString.Server);
        if (ConnectionString.Port > 0)
        {
            result.Append(',');
            result.Append(ConnectionString.Port);
        }

        if (!string.IsNullOrEmpty(ConnectionString.Location))
        {
            result.Append('\\');
            result.Append(ConnectionString.Location);
        }

        result.Append(';');
        if (string.IsNullOrEmpty(ConnectionString.UserName))
        {
            result.Append("Trusted_Connection=yes;");
        }
        else
        {
            result.Append("UID=" + ConnectionString.UserName + ";");
            result.Append("PWD=" + ConnectionString.UserName + ";");
        }

        result.Append("Encrypt=" + requireSSL + ";");
        return result.ToString();
    }

    /// <inheritdoc/>
    protected override IDbConnection GetDbConnectionType()
    {
        var flags = AppDom.LoadFlags.NoException | (AllowAssemblyLoad ? AppDom.LoadFlags.LoadAssemblies : 0);
        var type = AppDom.FindType("System.Data.SqlClient.SqlConnection", "System.Data.SqlClient", flags) ?? throw new InvalidOperationException("Could not find any SqlClient type!");
        return (Activator.CreateInstance(type) as IDbConnection) ?? throw new InvalidOperationException($"CreateInstance({type}) returned null!");
    }

    #endregion Protected Methods

    #region Protected Internal Properties

    /// <inheritdoc/>
    protected internal override bool DBConnectionCanChangeDataBase => true;

    #endregion Protected Internal Properties

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="MssqlStorage"/> class.</summary>
    /// <param name="connectionString">the connection details.</param>
    /// <param name="flags">The connection flags.</param>
    public MssqlStorage(ConnectionString connectionString, ConnectionFlags flags = default)
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
            var rows = QueryUnchecked("EXEC sdatabases;", "master", "sdatabases");
            foreach (var row in rows)
            {
                var databaseName = row[0]?.ToString() ?? throw new InvalidOperationException("information_schema did not return database name!");
                switch (databaseName)
                {
                    case "master":
                    case "model":
                    case "msdb":
                    case "tempdb":
                        continue;
                    default:
                        result.Add(databaseName);
                        continue;
                }
            }

            return result;
        }
    }

    /// <inheritdoc/>
    public override TimeSpan DateTimePrecision => TimeSpan.FromMilliseconds(4);

    /// <inheritdoc/>
    public override string ParameterPrefix => "@";

    /// <inheritdoc/>
    public override bool SupportsAllFieldsGroupBy => true;

    /// <inheritdoc/>
    public override bool SupportsNamedParameters => true;

    /// <inheritdoc/>
    public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1) - new TimeSpan(1);

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override IDatabase CreateDatabase(string databaseName)
    {
        if (databaseName.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Database name contains invalid chars!");
        }

        Execute(database: "information_schema", table: "SCHEMATA", cmd: "CREATE DATABASE " + databaseName);
        return GetDatabase(databaseName);
    }

    /// <inheritdoc/>
    public override void DeleteDatabase(string database)
    {
        if (database.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Database name contains invalid chars!");
        }

        Execute(database: "information_schema", table: "SCHEMATA", cmd: "DROP DATABASE " + database);
    }

    /// <inheritdoc/>
    public override string EscapeFieldName(IFieldProperties field) => "[" + field.NameAtDatabase + "]";

    /// <inheritdoc/>
    public override int Execute(SqlCmd cmd, string? database = null, string? table = null)
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

                var result = command.ExecuteNonQuery();
                if (result == 0)
                {
                    throw new InvalidOperationException();
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

                Trace.TraceInformation("<red>{3}<default> Error during Execute(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}", database,
                    table, i, ex.Message);
            }
            finally
            {
                ReturnConnection(ref connection, error);
            }
        }
    }

    /// <inheritdoc/>
    public override string FQTN(string database, string table) => "[" + database + "].[dbo].[" + table + "]";

    /// <inheritdoc/>
    public override IDatabase GetDatabase(string databaseName)
    {
        if (!HasDatabase(databaseName))
        {
            throw new DataException("Database does not exist!");
        }

        return new MssqlDatabase(this, databaseName);
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
            case DataType.Int8:
                var result = field.Clone();
                result.TypeAtDatabase = DataType.Int16;
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

        if (field.DataType == DataType.Int8)
        {
            return Convert.ToInt16(localValue, Culture);
        }

        if (field.DataType == DataType.Decimal)
        {
            if (localValue is null) return null;
            double preDecimal = 28;
            double valDecimal = 8;
            if (field.MaximumLength != 0)
            {
                preDecimal = Math.Truncate(field.MaximumLength);
                valDecimal = field.MaximumLength - preDecimal;
            }

            var max = (decimal)Math.Pow(10, preDecimal - valDecimal);
            var val = (decimal)localValue;
            if (val >= max)
            {
                throw new ArgumentOutOfRangeException(field.Name, $"Field {field.Name} with value {localValue} is greater than the maximum of {max}!");
            }

            if (val <= -max)
            {
                throw new ArgumentOutOfRangeException(field.Name, $"Field {field.Name} with value {localValue} is smaller than the minimum of {-max}!");
            }
        }

        return base.GetDatabaseValue(field, localValue);
    }

    /// <inheritdoc/>
    public override decimal GetDecimalPrecision(float count)
    {
        if (count == 0)
        {
            count = 28.08f;
        }

        return base.GetDecimalPrecision(count);
    }

    /// <inheritdoc/>
    public override bool HasDatabase(string databaseName)
    {
        if (databaseName.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Database name contains invalid chars!");
        }

        foreach (var name in DatabaseNames)
        {
            if (string.Equals(databaseName, name, StringComparison.CurrentCultureIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    #endregion Public Methods
}
