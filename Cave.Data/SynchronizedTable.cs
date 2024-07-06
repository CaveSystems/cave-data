using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides a synchronized wrapper for table instances.</summary>
    public class SynchronizedTable : ITable
    {
        #region Public Constructors

        /// <summary>Initializes a new instance of the <see cref="SynchronizedTable"/> class.</summary>
        /// <param name="table">The table to synchronize.</param>
        public SynchronizedTable(ITable table) =>
            BaseTable = table is not SynchronizedTable ? table : throw new ArgumentException("Table is already synchronized!");

        #endregion Public Constructors

        #region Public Properties

        /// <summary>Gets the base table used.</summary>
        public ITable BaseTable { get; }

        /// <inheritdoc/>
        public IDatabase Database => BaseTable.Database;

        /// <inheritdoc/>
        public TableFlags Flags => BaseTable.Flags;

        /// <inheritdoc/>
        public RowLayout Layout => BaseTable.Layout;

        /// <inheritdoc/>
        public string Name => BaseTable.Name;

        /// <inheritdoc/>
        public long RowCount
        {
            get
            {
                lock (BaseTable)
                {
                    return BaseTable.RowCount;
                }
            }
        }

        /// <inheritdoc/>
        public int SequenceNumber => BaseTable.SequenceNumber;

        /// <inheritdoc/>
        public IStorage Storage => BaseTable.Storage;

        #endregion Public Properties

        #region Public Methods

        /// <inheritdoc/>
        public void Clear()
        {
            lock (BaseTable)
            {
                BaseTable.Clear();
            }
        }

        /// <inheritdoc/>
        public int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = default)
        {
            lock (BaseTable)
            {
                return BaseTable.Commit(transactions, flags);
            }
        }

        /// <inheritdoc/>
        public void Connect(IDatabase database, TableFlags flags, RowLayout layout)
        {
            lock (BaseTable)
            {
                BaseTable.Connect(database, flags, layout);
            }
        }

        /// <inheritdoc/>
        public long Count(Search search = null, ResultOption resultOption = null)
        {
            lock (BaseTable)
            {
                return BaseTable.Count(search, resultOption);
            }
        }

        /// <inheritdoc/>
        public void Delete(Row row)
        {
            lock (BaseTable)
            {
                BaseTable.Delete(row);
            }
        }

        /// <inheritdoc/>
        public void Delete(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Delete(rows);
            }
        }

        /// <inheritdoc/>
        public IList<TValue> Distinct<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.Distinct<TValue>(fieldName, search);
            }
        }

        /// <inheritdoc/>
        public bool Exist(Search search)
        {
            lock (BaseTable)
            {
                return BaseTable.Exist(search);
            }
        }

        /// <inheritdoc/>
        public bool Exist(Row row)
        {
            lock (BaseTable)
            {
                return BaseTable.Exist(row);
            }
        }

        /// <inheritdoc/>
        public Row GetRow(Search search = null, ResultOption resultOption = null)
        {
            lock (BaseTable)
            {
                return BaseTable.GetRow(search, resultOption);
            }
        }

        /// <inheritdoc/>
        public Row GetRowAt(int index)
        {
            lock (BaseTable)
            {
                return BaseTable.GetRowAt(index);
            }
        }

        /// <inheritdoc/>
        public IList<Row> GetRows()
        {
            lock (BaseTable)
            {
                return BaseTable.GetRows();
            }
        }

        /// <inheritdoc/>
        public IList<Row> GetRows(Search search = null, ResultOption resultOption = null)
        {
            lock (BaseTable)
            {
                return BaseTable.GetRows(search, resultOption);
            }
        }

        /// <inheritdoc/>
        public IList<TValue> GetValues<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.GetValues<TValue>(fieldName, search);
            }
        }

        /// <inheritdoc/>
        public Row Insert(Row row)
        {
            lock (BaseTable)
            {
                return BaseTable.Insert(row);
            }
        }

        /// <inheritdoc/>
        public void Insert(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Insert(rows);
            }
        }

        /// <inheritdoc/>
        public TValue? Maximum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.Maximum<TValue>(fieldName, search);
            }
        }

        /// <inheritdoc/>
        public TValue? Minimum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.Minimum<TValue>(fieldName, search);
            }
        }

        /// <inheritdoc/>
        public void Replace(Row row)
        {
            lock (BaseTable)
            {
                BaseTable.Replace(row);
            }
        }

        /// <inheritdoc/>
        public void Replace(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Replace(rows);
            }
        }

        /// <inheritdoc/>
        public void SetValue(string fieldName, object value)
        {
            lock (BaseTable)
            {
                BaseTable.SetValue(fieldName, value);
            }
        }

        /// <inheritdoc/>
        public double Sum(string fieldName, Search search = null)
        {
            lock (BaseTable)
            {
                return BaseTable.Sum(fieldName, search);
            }
        }

        /// <inheritdoc/>
        public int TryDelete(Search search)
        {
            lock (BaseTable)
            {
                return BaseTable.TryDelete(search);
            }
        }

        /// <inheritdoc/>
        public void Update(Row row)
        {
            lock (BaseTable)
            {
                BaseTable.Update(row);
            }
        }

        /// <inheritdoc/>
        public void Update(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Update(rows);
            }
        }

        /// <inheritdoc/>
        public virtual void UseLayout(RowLayout layout)
        {
            lock (BaseTable)
            {
                BaseTable.UseLayout(layout);
            }
        }

        #endregion Public Methods
    }
}
