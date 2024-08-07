﻿using System;
using System.IO;
using System.Text;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Code generator for IDatabase instances.</summary>
    public class DatabaseInterfaceGenerator
    {
        #region Nested type: TableInfo

        class TableInfo
        {
            #region Properties

            public string ClassName { get; set; }
            public string GetterName { get; set; }
            public string TableName { get; set; }

            #endregion

            #region Overrides

            public override bool Equals(object obj)
            {
                if (obj is TableInfo other)
                {
                    // intentionally using or to check for duplicates
                    return
                        (other.TableName == TableName) ||
                        (other.ClassName == ClassName) ||
                        (other.GetterName == GetterName);
                }

                return false;
            }

            public override int GetHashCode() => TableName.GetHashCode();

            #endregion
        }

        #endregion

        #region Static

        string GetName(string text) => NamingStrategy.GetNameByStrategy(text);

        #endregion

        readonly string footer;
        readonly string header;
        readonly Set<TableInfo> tables = new();

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="DatabaseInterfaceGenerator" /> class.</summary>
        /// <param name="database">Database to use.</param>
        /// <param name="className">Name of the class to generate (optional).</param>
        public DatabaseInterfaceGenerator(IDatabase database, string className = null) : this(database, className, null) { }

        /// <summary>Initializes a new instance of the <see cref="DatabaseInterfaceGenerator" /> class.</summary>
        /// <param name="database">Database to use.</param>
        /// <param name="className">Name of the class to generate (optional).</param>
        /// <param name="nameSpace">The namespace to use for all classes (defaults to "Database").</param>
        public DatabaseInterfaceGenerator(IDatabase database, string className = null, string nameSpace = null)
        {
            var generator = typeof(DatabaseInterfaceGenerator);
            Database = database ?? throw new ArgumentNullException(nameof(database));
            ClassName = className ?? GetName(database.Name) + "Db";
            NameSpace = nameSpace ?? "Database";
            var code = new StringBuilder();
            code.AppendLine("//-----------------------------------------------------------------------");
            code.AppendLine("// <summary>");
            code.AppendLine($"// Autogenerated Database Interface Class");
            code.AppendLine("// </summary>");
            code.AppendLine("// <auto-generated />");
            code.AppendLine("//-----------------------------------------------------------------------");
            code.AppendLine();
            code.AppendLine("using System;");
            code.AppendLine("using System.Globalization;");
            code.AppendLine("using System.CodeDom.Compiler;");
            code.AppendLine("using Cave.Data;");
            code.AppendLine();
            code.AppendLine($"namespace {NameSpace}");
            code.AppendLine("{");
            code.AppendLine($"\t/// <summary>Provides access to table structures for database {database.Name}.</summary>");
            code.AppendLine($"\t[GeneratedCode(\"{generator.FullName}\",\"{generator.Assembly.GetName().Version}\")]");
            code.AppendLine($"\tpublic static partial class {ClassName}");
            code.AppendLine("\t{");
            code.AppendLine("\t\tstatic IDatabase? database;");
            code.AppendLine();
            code.AppendLine("\t\t/// <summary>Gets the used database instance.</summary>");
            code.AppendLine("\t\tpublic static IDatabase Database => database ?? throw new InvalidOperationException(\"Database is not connected!\");");
            code.AppendLine();
            code.AppendLine("\t\t/// <summary>Gets or sets the flags used when accessing table instances.</summary>");
            code.AppendLine("\t\tpublic static TableFlags DefaultTableFlags { get; set; }");
            code.AppendLine();
            code.AppendLine($"\t\t/// <summary>Connects to the {database.Name} database.</summary>");
            code.AppendLine("\t\t/// <param name=\"storage\">IStorage instance to use.</param>");
            code.AppendLine("\t\t/// <param name=\"createIfNotExists\">Create the databaseName if its not already present.</param>");
            code.AppendLine("\t\t/// <returns>A new <see cref=\"IDatabase\" /> instance.</returns>");
            code.AppendLine("\t\tpublic static void Connect(IStorage storage, bool createIfNotExists = false)");
            code.AppendLine("\t\t{");
            code.AppendLine($"\t\t\tdatabase = storage.GetDatabase(\"{database.Name}\", createIfNotExists);");
            code.AppendLine("\t\t}");
            code.AppendLine();
            code.AppendLine("\t\t/// <summary>Gets or sets the function used to retrieve tables from the database.</summary>");
            code.AppendLine("\t\tpublic static Func<string, ITable> GetTable { get; set; } = (tableName) => Database.GetTable(tableName, DefaultTableFlags);");

            header = code.ToString();
            code = new StringBuilder();
            code.AppendLine("\t}");
            code.AppendLine("}");
            footer = code.ToString();
        }

        #endregion

        #region Properties

        /// <summary>Gets the name of the generated class.</summary>
        public string ClassName { get; }

        /// <summary>Gets the used database instance.</summary>
        public IDatabase Database { get; }

        /// <summary>Gets the namespace of the generated class.</summary>
        public string NameSpace { get; }

        /// <summary>Naming strategy for classes, properties, structures and fields.</summary>
        public NamingStrategy NamingStrategy { get; set; } = NamingStrategy.PascalCase;

        #endregion

        #region Members

        /// <summary>Adds a table to the database interface code. This does not generate the table class!</summary>
        /// <param name="table">The table to add.</param>
        /// <param name="className">Name of the (table) class to use (optional).</param>
        /// <param name="getterName">Name of the getter in the resulting class (optional).</param>
        public void Add(ITable table, string className = null, string getterName = null)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            if (Database != table.Database)
            {
                throw new ArgumentOutOfRangeException(nameof(table), "Database has to match!");
            }

            Add(table.Name, className, getterName);
        }

        /// <summary>Adds a table to the database interface code. This does not generate the table class!</summary>
        /// <param name="tableCodeResult">The table to add.</param>
        /// <param name="getterName">Name of the getter in the resulting class (optional).</param>
        public void Add(GenerateTableCodeResult tableCodeResult, string getterName = null) =>
            Add(className: tableCodeResult.ClassName, tableName: tableCodeResult.TableName, getterName: getterName);

        /// <summary>Adds a table to the code.</summary>
        /// <param name="tableName">Name of the table at the database.</param>
        /// <param name="className">Name of the (table) class to use.</param>
        /// <param name="getterName">Name of the getter in the resulting class (optional).</param>
        public void Add(string tableName, string className = null, string getterName = null) =>
            tables.Add(new TableInfo
            {
                TableName = tableName ?? throw new ArgumentNullException(nameof(tableName)),
                ClassName = className ?? GetName(Database.Name) + GetName(tableName) + "Row",
                GetterName = getterName ?? GetName(tableName)
            });

        /// <summary>Generates the interface code.</summary>
        /// <returns>Returns c# code.</returns>
        public string Generate()
        {
            var result = new StringBuilder();
            result.Append(header);
            foreach (var table in tables)
            {
                result.AppendLine();
                result.AppendLine($"\t\t/// <summary>Gets a new <see cref=\"ITable{{{table.ClassName}}}\"/> instance for accessing the <c>{table.TableName}</c> table.</summary>");
                result.AppendLine($"\t\tpublic static ITable<{table.ClassName}> {table.GetterName} => new Table<{table.ClassName}>(GetTable(\"{table.TableName}\"));");
            }

            result.Append(footer);
            return result.ToString();
        }

        /// <summary>Builds the csharp code file containing the row layout structure and adds it to the interface.</summary>
        /// <param name="table"></param>
        /// <param name="databaseName">The database name (only used for the structure name).</param>
        /// <param name="tableName">The table name (only used for the structure name).</param>
        /// <param name="className">The name of the class to generate.</param>
        /// <param name="structFile">The table struct file name (defaults to classname.cs).</param>
        /// <param name="getterName">Name of the table getter (optional).</param>
        public GenerateTableCodeResult GenerateTableStructFile(ITable table, string databaseName = null, string tableName = null, string className = null,
            string structFile = null, string getterName = null)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            var result = new TableInterfaceGenerator()
            {
                Layout = table.Layout,
                DatabaseName = databaseName ?? table.Database.Name,
                TableName = tableName ?? table.Name,
                ClassName = className,
                NameSpace = NameSpace,
                NamingStrategy = NamingStrategy,
            }.GenerateStructFile(structFile);
            Add(result, getterName);
            return result;
        }

        /// <summary>Saves the output of <see cref="Generate" /> to the specified filename.</summary>
        /// <param name="fileName">Filename to save to.</param>
        public void Save(string fileName = null)
        {
            fileName ??= ClassName + ".cs";

            File.WriteAllText(fileName, Generate());
        }

        #endregion
    }
}
