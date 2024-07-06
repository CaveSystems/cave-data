using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides an interface for database tables.</summary>
    public interface ITable
    {
        #region Public Properties

        /// <summary>Gets the database the table belongs to.</summary>
        IDatabase Database { get; }

        /// <summary>Gets the flags used when connecting to the table.</summary>
        TableFlags Flags { get; }

        /// <summary>Gets the RowLayout of the table.</summary>
        RowLayout Layout { get; }

        /// <summary>Gets the name of the table.</summary>
        string Name { get; }

        /// <summary>Gets the RowCount.</summary>
        /// <returns>
        /// Returns a value &gt;= 0 representing the number of rows available at the table. If the table does not exist or cannot be read a value &lt; 0 is returned.
        /// </returns>
        long RowCount { get; }

        /// <summary>Gets the sequence number (counting write commands on this table).</summary>
        /// <value>The sequence number.</value>
        int SequenceNumber { get; }

        /// <summary>Gets the storage engine the database belongs to.</summary>
        IStorage Storage { get; }

        #endregion Public Properties

        #region Public Methods

        /// <summary>Clears all rows of the table.</summary>
        void Clear();

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactions">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <returns>The number of transactions done or -1 if unknown.</returns>
        int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = default);

        /// <summary>Initializes the interface class. This is the first method to call after create.</summary>
        /// <param name="database">Database the table belongs to.</param>
        /// <param name="flags">Flags to use when connecting to the table.</param>
        /// <param name="layout">Layout of the table.</param>
        void Connect(IDatabase database, TableFlags flags, RowLayout layout);

        /// <summary>Counts the results of a given search.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>The number of rows found matching the criteria given.</returns>
        long Count(Search search = default, ResultOption resultOption = default);

        /// <summary>Deletes a row at the table.</summary>
        /// <param name="row">The row to update.</param>
        void Delete(Row row);

        /// <summary>Deletes rows at the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        void Delete(IEnumerable<Row> rows);

        /// <summary>Obtains all different fieldName values of a given fieldName.</summary>
        /// <typeparam name="TValue">The returned value type.</typeparam>
        /// <param name="fieldName">The fieldName.</param>
        /// <param name="search">The search.</param>
        /// <returns>A new <see cref="List{TValue}"/>.</returns>
        IList<TValue> Distinct<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable;

        /// <summary>Searches the table using the specified search.</summary>
        /// <param name="search">The search to perform.</param>
        /// <returns>True if a matching dataset exists.</returns>
        bool Exist(Search search);

        /// <summary>Searches the table for a raw with the same identifiers.</summary>
        /// <param name="row">The row to search.</param>
        /// <returns>True if a matching dataset exists.</returns>
        bool Exist(Row row);

        /// <summary>Searches the table for a single row with given search.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>The row found.</returns>
        Row GetRow(Search search = default, ResultOption resultOption = default);

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index. Note that indices may change on
        /// each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The index of the row to be fetched.</param>
        /// <returns>The row.</returns>
        Row GetRowAt(int index);

        /// <summary>Gets all rows present at the table.</summary>
        /// <returns>Returns a new list of rows.</returns>
        IList<Row> GetRows();

        /// <summary>Searches the table for rows with given search.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>The rows found.</returns>
        IList<Row> GetRows(Search search = default, ResultOption resultOption = default);

        /// <summary>Obtains all different fieldName values of a given fieldName.</summary>
        /// <typeparam name="TValue">The returned value type.</typeparam>
        /// <param name="fieldName">The fieldName.</param>
        /// <param name="search">The search.</param>
        /// <returns>A new <see cref="List{TValue}"/>.</returns>
        IList<TValue> GetValues<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable;

        /// <summary>Inserts a row into the table and returns the inserted row.</summary>
        /// <param name="row">The row to insert.</param>
        /// <returns>The inserted row.</returns>
        Row Insert(Row row);

        /// <summary>Inserts rows into the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        void Insert(IEnumerable<Row> rows);

        /// <summary>Gets the maximum value for the specified fieldName.</summary>
        /// <typeparam name="TValue">Value type.</typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="search">Search to apply.</param>
        /// <returns>The maximum value found or null.</returns>
        TValue? Maximum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable;

        /// <summary>Gets the minimum value for the specified fieldName.</summary>
        /// <typeparam name="TValue">Value type.</typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="search">Search to apply.</param>
        /// <returns>The maximum value found or null.</returns>
        TValue? Minimum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable;

        /// <summary>Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.</summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        void Replace(Row row);

        /// <summary>Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.</summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        void Replace(IEnumerable<Row> rows);

        /// <summary>Sets the specified value to the specified fieldName on all rows.</summary>
        /// <param name="fieldName">The fieldName.</param>
        /// <param name="value">The value to set.</param>
        void SetValue(string fieldName, object value);

        /// <summary>Calculates the sum of the specified fieldName name for all matching rows.</summary>
        /// <param name="fieldName">Name of the fieldName.</param>
        /// <param name="search">The search.</param>
        /// <returns>The sum of all values at the specified fieldName.</returns>
        double Sum(string fieldName, Search search = null);

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>The number of dataset deleted.</returns>
        int TryDelete(Search search);

        /// <summary>Updates a row at the table.</summary>
        /// <param name="row">The row to update.</param>
        void Update(Row row);

        /// <summary>Updates rows at the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        void Update(IEnumerable<Row> rows);

        /// <summary>Updates the layout of the table (applies local fieldName mappings and type conversion).</summary>
        /// <param name="layout">The new layout.</param>
        void UseLayout(RowLayout layout);

        #endregion Public Methods
    }
}
