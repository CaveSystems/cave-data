using System;
using System.Collections.Generic;
using System.Linq;
using Cave.Data.Sql;

namespace Cave.Data.Mysql
{
    /// <summary>Provides a mysql table implementation.</summary>
    public class MySqlTable : SqlTable
    {
        #region Static

        /// <summary>Connects to the specified database and tablename.</summary>
        /// <param name="database">Database to connect to.</param>
        /// <param name="flags">Flags used to connect to the table.</param>
        /// <param name="tableName">The table to connect to.</param>
        /// <returns>Returns a new <see cref="MySqlTable" /> instance.</returns>
        public static MySqlTable Connect(MySqlDatabase database, TableFlags flags, string tableName)
        {
            var table = new MySqlTable();
            table.Initialize(database, flags, tableName);
            return table;
        }

        #endregion

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="MySqlTable" /> class.</summary>
        protected MySqlTable() { }

        #endregion

        #region Overrides

        /// <inheritdoc />
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
            commandBuilder.AppendLine($"SELECT * FROM {FQTN} WHERE {Storage.EscapeFieldName(idField)} = LAST_INSERT_ID();");
        }

        #endregion

        #region Members

        /// <summary>Runs the optimize table command.</summary>
        /// <returns>Result strings.</returns>
        public string[] Optimize() => MysqlInternalCommand($"OPTIMIZE TABLE {FQTN}");

        /// <summary>Runs the repair table command.</summary>
        /// <returns>Result strings.</returns>
        public string[] Repair() => MysqlInternalCommand($"REPAIR TABLE {FQTN} EXTENDED");

        string[] MysqlInternalCommand(string cmd)
        {
            if (cmd == null)
            {
                throw new ArgumentNullException(nameof(cmd));
            }

            var results = new List<string>();
            var rows = Storage.Query(database: Database.Name, table: Name, cmd: cmd);
            foreach (var row in rows)
            {
                var i = Layout.GetFieldIndex("Msg_text", true);
                var text = row[i].ToString();
                i = Layout.GetFieldIndex("Msg_type", true);
                var type = row[i].ToString();
                results.Add($"{type} {text}");
            }

            return results.ToArray();
        }

        #endregion
    }
}
