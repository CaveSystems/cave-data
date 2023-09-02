using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Cave.Data
{
    /// <summary>
    /// Provides a thread safe table stored completely in memory.
    /// </summary>
    [DebuggerDisplay("{Name}")]
    public class ConcurrentTable : ITable
    {
        #region Private Fields

        readonly object writeLock = new();
        int readLock;

        #endregion Private Fields

        #region Private Methods

        void ReadLocked(Action action)
        {
            int v;
            lock (this)
            {
                v = Interlocked.Increment(ref readLock);
            }

            if (Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("ReadLock <green>enter <magenta>{0}", v);
            }

            try
            {
                action();
            }
            finally
            {
                v = Interlocked.Decrement(ref readLock);
                if (Storage.LogVerboseMessages)
                {
                    Trace.TraceInformation("ReadLock <red>exit <magenta>{0}", v);
                }
            }
        }

        TResult ReadLockedFunc<TResult>(Func<TResult> func)
        {
            TResult result = default;
            ReadLocked(() => result = func());
            return result;
        }

        void WriteLocked(Action action)
        {
            var wait = MaxWaitTime;
            if (Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("WriteLock <cyan>wait (read lock <magenta>{0}<default>)", readLock);
            }

            lock (writeLock)
            {
                Monitor.Enter(this);
                if (readLock < 0)
                {
                    throw new InvalidOperationException("Fatal readlock underflow, deadlock imminent!");
                }

                while (readLock > 0)
                {
                    if (wait > 0)
                    {
                        Stopwatch watch = null;
                        if (wait > 0)
                        {
                            watch = new Stopwatch();
                            watch.Start();
                        }

                        // spin until noone is reading anymore or spincount is reached
                        while ((readLock > 0) && (watch.ElapsedMilliseconds < wait))
                        {
                            Monitor.Exit(this);
                            Thread.Sleep(1);
                            Monitor.Enter(this);
                        }

                        // if spinning completed and we are still waiting on readers, keep lock until all readers are finished
                        while (readLock > 0)
                        {
                            Thread.Sleep(0);
                        }
                    }
                    else
                    {
                        // spin until noone is reading anymore, this may wait forever if there is no gap between reading processes
                        while (readLock > 0)
                        {
                            Monitor.Exit(this);
                            Thread.Sleep(1);
                            Monitor.Enter(this);
                        }
                    }
                }

                if (Storage.LogVerboseMessages)
                {
                    Trace.TraceInformation("WriteLock <green>acquired (read lock <magenta>{0}<default>)", readLock);
                }

                // write
                try
                {
                    action.Invoke();
                }
                finally
                {
                    Monitor.Exit(this);
                    if (Storage.LogVerboseMessages)
                    {
                        Trace.TraceInformation("WriteLock <red>exit (read lock <magenta>{0}<default>)", readLock);
                    }
                }
            }
        }

        TResult WriteLocked<TResult>(Func<TResult> func)
        {
            TResult result = default;
            WriteLocked(() => result = func());
            return result;
        }

        #endregion Private Methods

        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentTable"/> class.
        /// </summary>
        /// <param name="table">The table to synchronize.</param>
        public ConcurrentTable(ITable table) => BaseTable = table is not ConcurrentTable ? table : throw new ArgumentException("Table is already synchronized!");

        #endregion Public Constructors

        #region Public Properties

        /// <summary>
        /// Gets the base table used.
        /// </summary>
        public ITable BaseTable { get; }

        /// <summary>
        /// Gets or sets the maximum wait time (milliseconds) while waiting for a write lock.
        /// </summary>
        /// <value>The maximum wait time (milliseconds) while waiting for a write lock.</value>
        /// <remarks>
        /// By default the maximum wait time is set to 100 milliseconds in release builds. Disabling the maximum wait time results in writing operations blocked
        /// forever if there are no gaps between reading operations.
        /// </remarks>
        public int MaxWaitTime { get; set; } = 100;

        /// <inheritdoc/>
        public IDatabase Database => BaseTable.Database;

        /// <inheritdoc/>
        public TableFlags Flags => BaseTable.Flags;

        /// <inheritdoc/>
        public RowLayout Layout => BaseTable.Layout;

        /// <inheritdoc/>
        public string Name => BaseTable.Name;

        /// <inheritdoc/>
        public long RowCount => ReadLockedFunc(() => BaseTable.RowCount);

        /// <summary>
        /// Gets the sequence number.
        /// </summary>
        /// <value>The sequence number.</value>
        public int SequenceNumber => BaseTable.SequenceNumber;

        /// <inheritdoc/>
        public IStorage Storage => BaseTable.Storage;

        #endregion Public Properties

        #region Public Methods

        /// <inheritdoc/>
        public virtual void Clear() => WriteLocked(BaseTable.Clear);

        /// <inheritdoc/>
        public int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = default) => WriteLocked(() => BaseTable.Commit(transactions, flags));

        /// <inheritdoc/>
        public virtual void Connect(IDatabase database, TableFlags flags, RowLayout layout) => BaseTable.Connect(database, flags, layout);

        /// <inheritdoc/>
        public virtual long Count(Search search = default, ResultOption resultOption = default) => ReadLockedFunc(() => BaseTable.Count(search, resultOption));

        /// <inheritdoc/>
        public void Delete(Row row) => WriteLocked(() => BaseTable.Delete(row));

        /// <inheritdoc/>
        public void Delete(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Delete(rows));

        /// <inheritdoc/>
        public IList<TValue> Distinct<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable =>
            ReadLockedFunc(() => BaseTable.Distinct<TValue>(fieldName, search));

        /// <inheritdoc/>
        public bool Exist(Search search) => ReadLockedFunc(() => BaseTable.Exist(search));

        /// <inheritdoc/>
        public bool Exist(Row row) => ReadLockedFunc(() => BaseTable.Exist(row));

        /// <inheritdoc/>
        public Row GetRow(Search search = default, ResultOption resultOption = default) => ReadLockedFunc(() => BaseTable.GetRow(search, resultOption));

        /// <inheritdoc/>
        public Row GetRowAt(int index) => ReadLockedFunc(() => BaseTable.GetRowAt(index));

        /// <inheritdoc/>
        public IList<Row> GetRows(Search search = default, ResultOption resultOption = default) =>
            ReadLockedFunc(() => BaseTable.GetRows(search, resultOption));

        /// <inheritdoc/>
        public IList<Row> GetRows() => ReadLockedFunc(() => BaseTable.GetRows());

        /// <inheritdoc/>
        public IList<TValue> GetValues<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable =>
            ReadLockedFunc(() => BaseTable.GetValues<TValue>(fieldName, search));

        /// <inheritdoc/>
        public Row Insert(Row row) => WriteLocked(() => BaseTable.Insert(row));

        /// <inheritdoc/>
        public void Insert(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Insert(rows));

        /// <inheritdoc/>
        public TValue? Maximum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable =>
            ReadLockedFunc(() => BaseTable.Maximum<TValue>(fieldName, search));

        /// <inheritdoc/>
        public TValue? Minimum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable =>
            ReadLockedFunc(() => BaseTable.Minimum<TValue>(fieldName, search));

        /// <inheritdoc/>
        public void Replace(Row row) => WriteLocked(() => BaseTable.Replace(row));

        /// <inheritdoc/>
        public void Replace(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Replace(rows));

        /// <inheritdoc/>
        public void SetValue(string fieldName, object value) => WriteLocked(() => BaseTable.SetValue(fieldName, value));

        /// <inheritdoc/>
        public double Sum(string fieldName, Search search = null) => ReadLockedFunc(() => BaseTable.Sum(fieldName, search));

        /// <summary>
        /// Returns a <see cref="string"/> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string"/> that represents this instance.</returns>
        public override string ToString() => $"Table {Database.Name}.{Name} [{RowCount} Rows]";

        /// <inheritdoc/>
        public int TryDelete(Search search) => WriteLocked(() => BaseTable.TryDelete(search));

        /// <inheritdoc/>
        public void Update(Row row) => WriteLocked(() => BaseTable.Update(row));

        /// <inheritdoc/>
        public void Update(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Update(rows));

        /// <inheritdoc/>
        public virtual void UseLayout(RowLayout layout) => BaseTable.UseLayout(layout);

        #endregion Public Methods
    }
}
