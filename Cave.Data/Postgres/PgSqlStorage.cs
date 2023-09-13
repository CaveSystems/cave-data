using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Cave.Data.Sql;

namespace Cave.Data.Postgres
{
    /// <summary>
    /// Provides a postgre sql storage implementation. Attention: <see cref="float" /> variables stored at the mysqldatabase loose their
    /// last precision digit (a value of 1 may differ by &lt;= 0.000001f).
    /// </summary>
    public sealed class PgSqlStorage : SqlStorage
    {
        /// <summary>Gets the mysql storage version.</summary>
        public readonly Version Version;

        /// <summary>Gets the mysql storage version.</summary>
        public readonly string VersionString;

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="PgSqlStorage" /> class.</summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="flags">The connection flags.</param>
        public PgSqlStorage(ConnectionString connectionString, ConnectionFlags flags = default)
            : base(connectionString, flags)
        {
            VersionString = (string)QueryValue("SELECT VERSION()");
            var parts = VersionString.Split(' ');
            Version = new Version(parts[1]);
            Trace.TraceInformation($"pgsql version {Version}");
        }

        #endregion

        #region properties

        /// <inheritdoc />
        public override bool SupportsNamedParameters => true;

        /// <inheritdoc />
        public override bool SupportsAllFieldsGroupBy => true;

        /// <inheritdoc />
        public override string ParameterPrefix => "@_";

        /// <inheritdoc />
        public override bool SupportsNativeTransactions { get; } = true;

        /// <inheritdoc />
        public override float FloatPrecision => 0.00001f;

        /// <inheritdoc />
        public override TimeSpan DateTimePrecision => TimeSpan.FromSeconds(1);

        /// <inheritdoc />
        public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1);

        /// <inheritdoc />
        public override IList<string> DatabaseNames
        {
            get
            {
                var result = new List<string>();
                var rows = Query(database: "SCHEMATA", cmd: "SELECT datname FROM pg_database;");
                foreach (var row in rows)
                {
                    result.Add((string)row[0]);
                }

                return result;
            }
        }

        /// <inheritdoc />
        protected internal override bool DBConnectionCanChangeDataBase => true;

        #endregion

        #region functions

        /// <summary>Gets the postgresql name of the databaseName/table/field.</summary>
        /// <param name="name">Name of the object.</param>
        /// <returns>The name of the databaseName object.</returns>
        public static string GetObjectName(string name) => name.ReplaceInvalidChars(ASCII.Strings.Letters + ASCII.Strings.Digits, "_");

        /// <inheritdoc />
        public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            var result = field.Clone();
            result.NameAtDatabase = GetObjectName(field.Name);
            switch (field.DataType)
            {
                case DataType.UInt8:
                    result.TypeAtDatabase = DataType.Int16;
                    break;
                case DataType.UInt16:
                    result.TypeAtDatabase = DataType.Int32;
                    break;
                case DataType.UInt32:
                    result.TypeAtDatabase = DataType.Int64;
                    break;
                case DataType.UInt64:
                    result.TypeAtDatabase = DataType.Decimal;
                    break;
            }

            return result;
        }

        /// <inheritdoc />
        public override IDbConnection CreateNewConnection(string databaseName)
        {
            var connection = base.CreateNewConnection(databaseName);
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SET CLIENT_ENCODING TO 'UTF8'; SET NAMES 'UTF8';";
                if (LogVerboseMessages)
                {
                    LogQuery(command);
                }

                // return value is not globally defined. some implementations use it correctly, some dont use it, some return positive or negative result enum values
                // so we ignore this: int affectedRows =
                command.ExecuteNonQuery();
            }

            return connection;
        }

        /// <inheritdoc />
        public override string EscapeFieldName(IFieldProperties field) => "\"" + field.NameAtDatabase + "\"";

        /// <inheritdoc />
        public override string FQTN(string database, string table)
        {
            if (table.IndexOf('"') > -1)
            {
                throw new ArgumentException("Tablename is invalid!");
            }

            return "\"" + table + "\"";
        }

        /// <inheritdoc />
        public override bool HasDatabase(string databaseName)
        {
            var value = QueryValue(database: "SCHEMATA",
                cmd: "SELECT COUNT(*) FROM pg_database WHERE datname LIKE " + EscapeString(GetObjectName(databaseName)) + ";");
            return Convert.ToInt32(value, Culture) > 0;
        }

        /// <inheritdoc />
        public override IDatabase GetDatabase(string databaseName) =>
            HasDatabase(databaseName) ? new PgSqlDatabase(this, databaseName) : throw new ArgumentException("Database does not exist!");

        /// <inheritdoc />
        public override IDatabase CreateDatabase(string databaseName)
        {
            if (databaseName.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }

            var cmd =
                $"CREATE DATABASE {GetObjectName(databaseName)} WITH OWNER = {EscapeString(ConnectionString.UserName)} ENCODING 'UTF8' CONNECTION LIMIT = -1;";
            Execute(cmd);
            return GetDatabase(databaseName);
        }

        /// <inheritdoc />
        public override void DeleteDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }

            Execute($"DROP DATABASE {GetObjectName(database)};");
        }

        /// <inheritdoc />
        public override decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
            {
                count = 65.30f;
            }

            return base.GetDecimalPrecision(count);
        }

        /// <inheritdoc />
        protected override IDbConnection GetDbConnectionType()
        {
            var flags = AppDom.LoadFlags.NoException | (AllowAssemblyLoad ? AppDom.LoadFlags.LoadAssemblies : 0);
            var type = AppDom.FindType("Npgsql.NpgsqlConnection", "Npgsql", flags);
            return (IDbConnection)Activator.CreateInstance(type);
        }

        /// <inheritdoc />
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

            return
                "Host=" + ConnectionString.Server + ";" +
                "Username=" + ConnectionString.UserName + ";" +
                "Password=" + ConnectionString.Password + ";" +
                "Port=" + ConnectionString.GetPort(5432) + ";" +
                "Database=" + (database ?? "postgres") + ";" +
                "SSL Mode=" + (requireSSL ? "Require;" : "Prefer;");
        }

        #endregion
    }
}
