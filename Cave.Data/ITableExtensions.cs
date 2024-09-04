using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides extension functions for ITable instances.</summary>
public static class ITableExtensions
{
    #region Public Methods

    /// <summary>Counts the rows with specified field value combination.</summary>
    /// <param name="table">The table.</param>
    /// <param name="field">The field name to match.</param>
    /// <param name="value">The value to match.</param>
    /// <returns>The number of rows found matching the criteria given.</returns>
    public static long Count(this ITable table, string field, object value)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return table.Count(Search.FieldEquals(field, value), ResultOption.None);
    }

    /// <summary>Checks a given search for any data sets matching.</summary>
    /// <param name="table">The table.</param>
    /// <param name="field">The fields name.</param>
    /// <param name="value">The value.</param>
    /// <returns>Returns true if a data set exists, false otherwise.</returns>
    public static bool Exist(this ITable table, string field, object value)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return table.Exist(Search.FieldEquals(field, value));
    }

    /// <summary>Builds the csharp code containing the row layout structure.</summary>
    /// <param name="table">The table to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>Returns a string containing csharp code.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStruct instead")]
    public static GenerateTableCodeResult GenerateStruct(this ITable table, string databaseName, string tableName, string className, NamingStrategy namingStrategy) => new TableInterfaceGenerator()
    {
        Layout = table?.Layout ?? throw new ArgumentNullException(nameof(table)),
        TableName = tableName ?? table.Name,
        DatabaseName = databaseName ?? table.Database.Name,
        ClassName = className,
        NamingStrategy = namingStrategy
    }.GenerateStruct();

    /// <summary>Builds the csharp code containing the row layout structure.</summary>
    /// <param name="table">The table to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="nameSpace">The namespace to use for the class (defaults to "Database").</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>Returns a string containing csharp code.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStruct instead")]
    public static GenerateTableCodeResult GenerateStruct(this ITable table, string databaseName = null, string tableName = null, string className = null, string nameSpace = null, NamingStrategy namingStrategy = NamingStrategy.PascalCase) => new TableInterfaceGenerator()
    {
        Layout = table?.Layout ?? throw new ArgumentNullException(nameof(table)),
        TableName = tableName ?? table.Name,
        DatabaseName = databaseName ?? table.Database.Name,
        ClassName = className,
        NameSpace = nameSpace,
        NamingStrategy = namingStrategy
    }.GenerateStruct();

    /// <summary>Builds the csharp code containing the row layout structure.</summary>
    /// <param name="layout">The layout to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>Returns a string containing csharp code.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStruct instead")]
    public static GenerateTableCodeResult GenerateStruct(this RowLayout layout, string databaseName, string tableName, string className, NamingStrategy namingStrategy) => new TableInterfaceGenerator()
    {
        Layout = layout ?? throw new ArgumentNullException(nameof(layout)),
        TableName = tableName ?? layout.Name,
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName)),
        ClassName = className,
        NamingStrategy = namingStrategy
    }.GenerateStruct();

    /// <summary>Builds the csharp code containing the row layout structure.</summary>
    /// <param name="layout">The layout to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="nameSpace">The namespace to use for the class (defaults to "Database").</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>Returns a string containing csharp code.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStruct instead")]
    public static GenerateTableCodeResult GenerateStruct(this RowLayout layout, string databaseName, string tableName = null, string className = null, string nameSpace = null, NamingStrategy namingStrategy = NamingStrategy.PascalCase) => new TableInterfaceGenerator()
    {
        Layout = layout ?? throw new ArgumentNullException(nameof(layout)),
        TableName = tableName ?? layout.Name,
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName)),
        ClassName = className,
        NameSpace = nameSpace,
        NamingStrategy = namingStrategy
    }.GenerateStruct();

    /// <summary>Builds the csharp code file containing the row layout structure.</summary>
    /// <param name="table">The table to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="structFile">The struct file name (defaults to classname.cs).</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>The struct file name.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStructFile instead")]
    public static GenerateTableCodeResult GenerateStructFile(this ITable table, string databaseName, string tableName, string className, string structFile, NamingStrategy namingStrategy)
        => GenerateStruct(table.Layout, databaseName, tableName, className, namingStrategy).SaveStructFile(structFile);

    /// <summary>Builds the csharp code file containing the row layout structure.</summary>
    /// <param name="table">The table to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="structFile">The struct file name (defaults to classname.cs).</param>
    /// <param name="nameSpace">The namespace to use for the class (defaults to "Database").</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>The struct file name.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStructFile instead")]
    public static GenerateTableCodeResult GenerateStructFile(this ITable table, string databaseName = null, string tableName = null, string className = null, string structFile = null, string nameSpace = null, NamingStrategy namingStrategy = NamingStrategy.PascalCase)
        => GenerateStruct(table.Layout, databaseName ?? table.Database.Name, tableName, className, nameSpace, namingStrategy).SaveStructFile(structFile);

    /// <summary>Builds the csharp code file containing the row layout structure.</summary>
    /// <param name="layout">The layout to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="structFile">The struct file name (defaults to classname.cs).</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>The struct file name.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStructFile instead")]
    public static GenerateTableCodeResult GenerateStructFile(this RowLayout layout, string databaseName, string tableName, string className, string structFile, NamingStrategy namingStrategy)
        => GenerateStruct(layout, databaseName, tableName, className, namingStrategy).SaveStructFile(structFile);

    /// <summary>Builds the csharp code file containing the row layout structure.</summary>
    /// <param name="layout">The layout to use.</param>
    /// <param name="databaseName">The database name (only used for the structure name).</param>
    /// <param name="tableName">The table name (only used for the structure name).</param>
    /// <param name="className">The name of the class to generate.</param>
    /// <param name="structFile">The struct file name (defaults to classname.cs).</param>
    /// <param name="nameSpace">The namespace to use for the class (defaults to "Database").</param>
    /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
    /// <returns>The struct file name.</returns>
    [Obsolete("Use TableInterfaceGenerator.GenerateStructFile instead")]
    public static GenerateTableCodeResult GenerateStructFile(this RowLayout layout, string databaseName = null, string tableName = null, string className = null, string structFile = null, string nameSpace = null, NamingStrategy namingStrategy = NamingStrategy.PascalCase)
        => GenerateStruct(layout, databaseName, tableName, className, nameSpace, namingStrategy).SaveStructFile(structFile);

    /// <summary>Gets the string comparison to use for field name comparison.</summary>
    /// <param name="tableFlags">Flags to use for setting.</param>
    /// <returns>Returns a <see cref="StringComparison"/> value.</returns>
    public static StringComparison GetFieldNameComparison(this TableFlags tableFlags) => tableFlags.HasFlag(TableFlags.FieldNamesCaseInsensitive) ? StringComparison.InvariantCultureIgnoreCase : StringComparison.Ordinal;

    /// <summary>Gets the string comparison to use for field name comparison.</summary>
    /// <param name="table">Flags to use for setting.</param>
    /// <returns>Returns a <see cref="StringComparison"/> value.</returns>
    public static StringComparison GetFieldNameComparison(this ITable table) => GetFieldNameComparison(table.Flags);

    /// <summary>Searches the table for a single row with given field value combination.</summary>
    /// <param name="table">The table.</param>
    /// <param name="field">The field name to match.</param>
    /// <param name="value">The value to match.</param>
    /// <returns>The row found.</returns>
    public static Row GetRow(this ITable table, string field, object value)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return table.GetRow(Search.FieldEquals(field, value));
    }

    /// <summary>Gets a row using the id field</summary>
    /// <typeparam name="TIdentifier"></typeparam>
    /// <param name="table">The table.</param>
    /// <param name="id">The id</param>
    /// <returns>Returns the row found</returns>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ArgumentException"></exception>
    public static Row GetRow<TIdentifier>(this ITable table, TIdentifier id)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var idField = table.Layout.SingleIdentifier;
        if (idField is null) throw new ArgumentException("Table has no single unique id field!");
        return GetRow(table, idField.Name, id);
    }

    /// <summary>Searches the table for rows with given field value combinations.</summary>
    /// <param name="table">The table.</param>
    /// <param name="field">The field name to match.</param>
    /// <param name="value">The value to match.</param>
    /// <returns>The rows found.</returns>
    public static IList<Row> GetRows(this ITable table, string field, object value)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return table.GetRows(Search.FieldEquals(field, value));
    }

    /// <summary>Gets a row using the id field</summary>
    /// <typeparam name="TIdentifier"></typeparam>
    /// <param name="table">The table.</param>
    /// <param name="id">The id</param>
    /// <returns>Returns the row found</returns>
    /// <typeparam name="TStruct"></typeparam>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ArgumentException"></exception>
    public static TStruct GetStruct<TStruct, TIdentifier>(this ITable<TStruct> table, TIdentifier id)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var idField = table.Layout.SingleIdentifier;
        if (idField is null) throw new ArgumentException("Table has no single unique id field!");
        return table.GetStruct(Search.FieldEquals(idField.Name, id));
    }

    /// <summary>Searches the table for rows with given field value combinations.</summary>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    /// <param name="table">The table.</param>
    /// <param name="field">The fieldname to match.</param>
    /// <param name="value">The value to match.</param>
    /// <returns>The rows found.</returns>
    public static IList<TStruct> GetStructs<TStruct>(this ITable<TStruct> table, string field, object value)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return table.GetStructs(Search.FieldEquals(field, value));
    }

    /// <summary>Saves the generated code to a file and returns the updated result.</summary>
    /// <param name="result">Result to update.</param>
    /// <param name="structFile">Name of the file to write to (optional).</param>
    /// <returns>Returns an updated result instance.</returns>
    [Obsolete("Use TableInterfaceGenerator.SaveStructFile instead")]
    public static GenerateTableCodeResult SaveStructFile(this GenerateTableCodeResult result, string structFile = null)
    {
        if (result.FileName == null)
        {
            if (structFile == null)
            {
                result.FileName = result.ClassName + ".cs";
            }
            else
            {
                result.FileName = structFile;
            }
        }

        File.WriteAllText(result.FileName, result.Code);
        return result;
    }

    /// <summary>Saves the table to a dat stream.</summary>
    /// <param name="table">Table to save.</param>
    /// <param name="stream">The stream to save to.</param>
    public static void SaveTo(this ITable table, Stream stream)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        using var writer = new DatWriter(table.Layout, stream);
        writer.WriteTable(table);
    }

    /// <summary>Saves the table to a dat file.</summary>
    /// <param name="table">Table to save.</param>
    /// <param name="fileName">The filename to save to.</param>
    public static void SaveTo(this ITable table, string fileName)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        using var stream = File.Create(fileName);
        using var writer = new DatWriter(table.Layout, stream);
        writer.WriteTable(table);
        writer.Close();
    }

    /// <summary>Caches the whole table into memory and provides a new ITable instance.</summary>
    /// <param name="table">The table.</param>
    /// <returns>Returns a new memory table.</returns>
    public static MemoryTable ToMemory(this ITable table) => ToMemory(table, 0, 0);

    /// <summary>Caches the whole table into memory and provides a new ITable instance.</summary>
    /// <param name="table">The table.</param>
    /// <param name="flags">Flags for the new memory table</param>
    /// <param name="options">Options for the new memory table</param>
    /// <returns>Returns a new memory table.</returns>
    public static MemoryTable ToMemory(this ITable table, TableFlags flags = 0, MemoryTableOptions options = 0)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        Trace.TraceInformation("Copy {0} rows to memory table", table.RowCount);
        var result = MemoryTable.Create(table.Layout, flags, options);
        result.LoadTable(table);
        return result;
    }

    /// <summary>Caches the whole table into memory and provides a new ITable{TStruct} instance.</summary>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    /// <param name="table">The table.</param>
    /// <returns>Returns a new memory table.</returns>
    public static ITable<TStruct> ToMemory<TStruct>(this ITable<TStruct> table) where TStruct : struct => ToMemory(table, 0, 0);

    /// <summary>Caches the whole table into memory and provides a new ITable{TStruct} instance.</summary>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    /// <param name="table">The table.</param>
    /// <param name="flags">Flags for the new memory table</param>
    /// <param name="options">Options for the new memory table</param>
    /// <returns>Returns a new memory table.</returns>
    public static ITable<TStruct> ToMemory<TStruct>(this ITable<TStruct> table, TableFlags flags = 0, MemoryTableOptions options = 0)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        Trace.TraceInformation("Copy {0} rows to memory table", table.RowCount);
        var result = MemoryTable.Create(table.Layout, flags, options);
        result.LoadTable(table);
        return new Table<TStruct>(result);
    }

    /// <summary>Caches the whole table into memory and provides a new ITable{TStruct} instance.</summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    /// <param name="table">The table.</param>
    /// <returns>Returns a new memory table.</returns>
    public static ITable<TKey, TStruct> ToMemory<TKey, TStruct>(this ITable<TKey, TStruct> table) where TKey : IComparable<TKey> where TStruct : struct => ToMemory(table, 0, 0);

    /// <summary>Caches the whole table into memory and provides a new ITable{TStruct} instance.</summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    /// <param name="table">The table.</param>
    /// <param name="flags">Flags for the new memory table</param>
    /// <param name="options">Options for the new memory table</param>
    /// <returns>Returns a new memory table.</returns>
    public static ITable<TKey, TStruct> ToMemory<TKey, TStruct>(this ITable<TKey, TStruct> table, TableFlags flags = 0, MemoryTableOptions options = 0)
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var result = MemoryTable.Create(table.Layout, flags, options);
        result.LoadTable(table);
        return new Table<TKey, TStruct>(result);
    }

    /// <summary>Tries to delete the data set with the specified id.</summary>
    /// <param name="table">The table.</param>
    /// <param name="id">The identifier.</param>
    /// <returns>Returns true if the data set was removed, false otherwise.</returns>
    /// <typeparam name="TIdentifier">Identifier type. This has to be convertible to the database <see cref="DataType"/>.</typeparam>
    public static bool TryDelete<TIdentifier>(this ITable table, TIdentifier id)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var idField = table.Layout.SingleIdentifier;
        if (idField is null) throw new ArgumentException("Table has no single unique id field!");
        return table.TryDelete(idField.Name, id) > 0;
    }

    /// <summary>Tries to delete the data sets with the specified identifiers.</summary>
    /// <param name="table">The table.</param>
    /// <param name="ids">The identifiers.</param>
    /// <returns>The number of data sets removed, 0 if the database does not support deletion count or no data set was removed.</returns>
    /// <typeparam name="TIdentifier">Identifier type. This has to be convertible to the database <see cref="DataType"/>.</typeparam>
    public static int TryDelete<TIdentifier>(this ITable table, IEnumerable<TIdentifier> ids)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var idField = table.Layout.SingleIdentifier;
        if (idField is null) throw new ArgumentException("Table has no single unique id field!");
        return table.TryDelete(Search.FieldIn(idField.Name, ids));
    }

    /// <summary>Removes all rows from the table matching the given search.</summary>
    /// <param name="table">The table.</param>
    /// <param name="field">The field name to match.</param>
    /// <param name="value">The value to match.</param>
    /// <returns>The number of data sets deleted.</returns>
    public static int TryDelete(this ITable table, string field, object value)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        return table.TryDelete(Search.FieldEquals(field, value));
    }

    /// <summary>Tries to get the row with the specified <paramref name="value"/> from <paramref name="table"/>.</summary>
    /// <typeparam name="TStruct">The row structure type.</typeparam>
    /// <param name="table">The table.</param>
    /// <param name="field">The fieldname to match.</param>
    /// <param name="value">The value to match.</param>
    /// <param name="row">Returns the result row.</param>
    /// <returns>Returns true on success, false otherwise.</returns>
    /// <exception cref="InvalidOperationException">The result sequence contains more than one element.</exception>
    public static bool TryGetStruct<TStruct>(this ITable<TStruct> table, string field, object value, out TStruct row)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var result = GetStructs(table, field, value);
        if (result.Count > 0)
        {
            row = result.Single();
            return true;
        }

        row = default;
        return false;
    }

    /// <summary>Tries to get the row with the specified <paramref name="key"/> from <paramref name="table"/>.</summary>
    /// <param name="table">The table.</param>
    /// <param name="key">The key to match.</param>
    /// <param name="row">Returns the result row.</param>
    /// <returns>Returns true on success, false otherwise.</returns>
    /// <exception cref="InvalidOperationException">The result sequence contains more than one element.</exception>
    public static bool TryGetStruct<TKey, TStruct>(this ITable<TStruct> table, TKey key, out TStruct row)
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var idField = table.Layout.SingleIdentifier;
        if (idField is null) throw new ArgumentException("Table has no single unique id field!");
        var result = table.GetStructs(idField.Name, key);
        if (result.Count > 0)
        {
            row = result.Single();
            return true;
        }

        row = default;
        return false;
    }

    /// <summary>Tries to get the row with the specified <paramref name="key"/> from <paramref name="table"/>.</summary>
    /// <param name="table">The table.</param>
    /// <param name="key">The key to match.</param>
    /// <param name="row">Returns the result row.</param>
    /// <returns>Returns true on success, false otherwise.</returns>
    /// <exception cref="InvalidOperationException">The result sequence contains more than one element.</exception>
    public static bool TryGetStruct<TKey, TStruct>(this ITable<TKey, TStruct> table, TKey key, out TStruct row)
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var result = table.GetStructs(new[] { key });
        if (result.Count > 0)
        {
            row = result.Single();
            return true;
        }

        row = default;
        return false;
    }

    /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
    /// <param name="table">The table.</param>
    /// <param name="row">The row.</param>
    /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
    public static bool TryInsert(this ITable table, Row row)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        try
        {
            table.Insert(row);
            return true;
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"Exception during TryInsert(): {ex}");
            return false;
        }
    }

    /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
    /// <param name="table">The table.</param>
    /// <param name="row">The row.</param>
    /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
    public static bool TryInsert<TStruct>(this ITable<TStruct> table, TStruct row)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        try
        {
            table.Insert(row);
            return true;
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"Exception during TryInsert(): {ex}");
            return false;
        }
    }

    /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
    /// <param name="table">The table.</param>
    /// <param name="row">The row.</param>
    /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
    public static bool TryUpdate(this ITable table, Row row)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        try
        {
            table.Update(row);
            return true;
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"Exception during TryUpdate(): {ex}");
            return false;
        }
    }

    /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
    /// <param name="table">The table.</param>
    /// <param name="row">The row.</param>
    /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
    public static bool TryUpdate<TStruct>(this ITable<TStruct> table, TStruct row)
         where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        try
        {
            table.Update(row);
            return true;
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"Exception during TryUpdate(): {ex}");
            return false;
        }
    }

    #endregion Public Methods
}
