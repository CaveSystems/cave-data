using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a class caching all results fetched from another table during usage. This class can be used to replace any other <see cref="ITable"/> instance
    /// to build up a row cache on the fly during normal usage.
    /// </summary>
    public class RowCacheBuilder : ITable
    {
        #region Public Constructors

        /// <inheritdoc/>
        public RowCacheBuilder(ITable table)
        {
            Table = table;
            Cache = MemoryTable.Create(table.Layout);
        }

        #endregion Public Constructors

        #region Public Properties

        /// <summary>
        /// The backing table containing the cached rows.
        /// </summary>
        public MemoryTable Cache { get; }

        /// <summary>
        /// Table to run queries on.
        /// </summary>
        public ITable Table { get; }

        /// <inheritdoc/>
        public IDatabase Database => Table.Database;

        /// <inheritdoc/>
        public TableFlags Flags => Table.Flags;

        /// <inheritdoc/>
        public RowLayout Layout => Table.Layout;

        /// <inheritdoc/>
        public string Name => Table.Name;

        /// <inheritdoc/>
        public long RowCount => Table.RowCount;

        /// <inheritdoc/>
        public int SequenceNumber => Table.SequenceNumber;

        /// <inheritdoc/>
        public IStorage Storage => Table.Storage;

        #endregion Public Properties

        #region Public Methods

        /// <inheritdoc/>
        public void Clear()
        {
            Table.Clear();
            Cache.Clear();
        }

        /// <inheritdoc/>
        public int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = TransactionFlags.None)
        {
            var result = Table.Commit(transactions, flags);
            Cache.Commit(transactions, TransactionFlags.NoExceptions);
            return result;
        }

        /// <inheritdoc/>
        public void Connect(IDatabase database, TableFlags flags, RowLayout layout) => Table.Connect(database, flags, layout);

        /// <inheritdoc/>
        public long Count(Search search = null, ResultOption resultOption = null) => Table.Count(search, resultOption);

        /// <inheritdoc/>
        public void Delete(Row row)
        {
            Table.Delete(row);
            Cache.TryDelete(row);
        }

        /// <inheritdoc/>
        public void Delete(IEnumerable<Row> rows)
        {
            Table.Delete(rows);
            Cache.TryDelete(rows);
        }

        /// <inheritdoc/>
        public IList<TValue> Distinct<TValue>(string fieldName, Search search = null) where TValue : struct, IComparable => Table.Distinct<TValue>(fieldName, search);

        /// <inheritdoc/>
        public bool Exist(Search search) => Table.Exist(search);

        /// <inheritdoc/>
        public bool Exist(Row row) => Table.Exist(row);

        /// <inheritdoc/>
        public Row GetRow(Search search = null, ResultOption resultOption = null)
        {
            var row = Table.GetRow(search, resultOption);
            if (resultOption.Mode != ResultOptionMode.Group)
            {
                Cache.Replace(row);
            }
            return row;
        }

        /// <inheritdoc/>
        public Row GetRowAt(int index)
        {
            var row = Table.GetRowAt(index);
            Cache.Replace(row);
            return row;
        }

        /// <summary>
        /// Creates a <see cref="RowCache{TKey, TStruct}"/> using <see cref="Table"/>.
        /// </summary>
        /// <typeparam name="TKey">Key identifier type.</typeparam>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <returns>Returns a new instance using <see cref="Table"/> as datasource.</returns>
        public RowCache<TKey, TStruct> GetRowCache<TKey, TStruct>()
            where TKey : IComparable<TKey> where TStruct : struct
            => new(Table);

        /// <summary>
        /// Creates a <see cref="RowCache{TKey, TStruct, TTarget}"/> using <see cref="Table"/>.
        /// </summary>
        /// <typeparam name="TKey">Key identifier type.</typeparam>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <typeparam name="TTarget">Result and cache item type.</typeparam>
        /// <param name="func">Function to convert from <typeparamref name="TStruct"/> to <typeparamref name="TTarget"/>.</param>
        /// <returns>Returns a new instance using <see cref="Table"/> as datasource.</returns>
        public RowCache<TKey, TStruct, TTarget> GetRowCache<TKey, TStruct, TTarget>(RowCache<TKey, TStruct, TTarget>.RowCacheConvertFunction func)
            where TKey : IComparable<TKey> where TStruct : struct where TTarget : class
            => new(Table, func);

        /// <inheritdoc/>
        public IList<Row> GetRows()
        {
            var rows = Table.GetRows();
            Cache.Replace(rows);
            return rows;
        }

        /// <inheritdoc/>
        public IList<Row> GetRows(Search search = null, ResultOption resultOption = null)
        {
            var rows = Table.GetRows(search, resultOption);
            if (resultOption.Mode != ResultOptionMode.Group)
            {
                Cache.Replace(rows);
            }
            return rows;
        }

        /// <inheritdoc/>
        public IList<TValue> GetValues<TValue>(string fieldName, Search search = null) where TValue : struct, IComparable => Table.GetValues<TValue>(fieldName, search);

        /// <inheritdoc/>
        public Row Insert(Row row)
        {
            var result = Table.Insert(row);
            Cache.Insert(result);
            return result;
        }

        /// <inheritdoc/>
        public void Insert(IEnumerable<Row> rows)
        {
            Table.Insert(rows);
            Cache.Insert(rows);
        }

        /// <inheritdoc/>
        public TValue? Maximum<TValue>(string fieldName, Search search = null) where TValue : struct, IComparable => Table.Maximum<TValue>(fieldName, search);

        /// <inheritdoc/>
        public TValue? Minimum<TValue>(string fieldName, Search search = null) where TValue : struct, IComparable => Table.Minimum<TValue>(fieldName, search);

        /// <inheritdoc/>
        public void Replace(Row row)
        {
            Table.Replace(row);
            Cache.Replace(row);
        }

        /// <inheritdoc/>
        public void Replace(IEnumerable<Row> rows)
        {
            Table.Replace(rows);
            Cache.Replace(rows);
        }

        /// <inheritdoc/>
        public void SetValue(string fieldName, object value)
        {
            Table.SetValue(fieldName, value);
            Cache.SetValue(fieldName, value);
        }

        /// <inheritdoc/>
        public double Sum(string fieldName, Search search = null) => Table.Sum(fieldName, search);

        /// <inheritdoc/>
        public int TryDelete(Search search)
        {
            var result = Table.TryDelete(search);
            Cache.TryDelete(search);
            return result;
        }

        /// <inheritdoc/>
        public void Update(Row row)
        {
            Table.Update(row);
            Cache.Replace(row);
        }

        /// <inheritdoc/>
        public void Update(IEnumerable<Row> rows)
        {
            Table.Update(rows);
            Cache.Replace(rows);
        }

        /// <inheritdoc/>
        public void UseLayout(RowLayout layout)
        {
            Table.UseLayout(layout);
            Cache.UseLayout(layout);
        }

        #endregion Public Methods
    }
}
