using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides an interface for a table of structs (rows).</summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public interface ITable<TKey, TStruct> : ITable<TStruct>
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        /// <summary>Gets the row with the specified ID.</summary>
        /// <param name="id">The identifier.</param>
        /// <returns>The row found.</returns>
        TStruct this[TKey id] { get; }

        /// <summary>Checks a given ID for existance.</summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        bool Exist(TKey id);

        /// <summary>Removes a row from the table.</summary>
        /// <param name="id">The dataset ID to remove.</param>
        void Delete(TKey id);

        /// <summary>Removes rows from the table using a transaction.</summary>
        /// <param name="ids">The dataset IDs to remove.</param>
        void Delete(IEnumerable<TKey> ids);

        /// <summary>Gets a row from the table.</summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>The row.</returns>
        Row GetRow(TKey id);

        /// <summary>Gets the rows with the given identifiers.</summary>
        /// <param name="ids">Identifiers of the rows to fetch from the table.</param>
        /// <returns>The rows.</returns>
        IList<Row> GetRows(IEnumerable<TKey> ids);

        /// <summary>Gets a row from the table.</summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>The row.</returns>
        TStruct GetStruct(TKey id);

        /// <summary>Gets the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>The rows.</returns>
        IList<TStruct> GetStructs(IEnumerable<TKey> ids);

        /// <summary>Gets a dictionary with all datasets.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>A readonly dictionary.</returns>
        IDictionary<TKey, TStruct> GetDictionary(Search search = null, ResultOption resultOption = null);
    }
}
