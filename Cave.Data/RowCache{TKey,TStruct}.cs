using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a row cache class working with row structures.
    /// </summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public class RowCache<TKey, TStruct> : IRowCache where TKey : IComparable<TKey> where TStruct : struct
    {
        #region Private Fields

        readonly Dictionary<TKey, TStruct?> cache = new();
        readonly ITable<TKey, TStruct> table;

        #endregion Private Fields

        #region Public Constructors

        /// <summary>
        /// Creates a new row cache using the specified table.
        /// </summary>
        /// <param name="table">Table to read rows from.</param>
        public RowCache(ITable table) => this.table = new Table<TKey, TStruct>(table);

        #endregion Public Constructors

        #region Public Properties

        /// <inheritdoc/>
        public long HitCount { get; set; }

        /// <inheritdoc/>
        public long MissCount { get; set; }

        /// <inheritdoc/>
        public long NotFoundCount { get; set; }

        #endregion Public Properties

        #region Public Methods

        /// <inheritdoc/>
        public void Clear()
        {
            lock (this)
            {
                HitCount = 0;
                MissCount = 0;
                NotFoundCount = 0;
                cache.Clear();
            }
        }

        /// <summary>
        /// Gets the row with the specified identifier.
        /// </summary>
        /// <param name="id">Row identifier.</param>
        /// <returns>Returns the row structure or null.</returns>
        public TStruct? Get(TKey id) => (TryGetStruct(id, out var value)) ? value : null;

        /// <summary>
        /// Tries to get the value with the specified identifier.
        /// </summary>
        /// <param name="id">The identifier value.</param>
        /// <param name="value">Returns the result value.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        public bool TryGetStruct(TKey id, out TStruct value)
        {
            lock (this)
            {
                if (cache.TryGetValue(id, out var row) && row != null)
                {
                    value = row.Value;
                    HitCount++;
                    return true;
                }

                MissCount++;
                if (table.TryGetStruct(id, out value))
                {
                    cache[id] = value;
                    return true;
                }

                NotFoundCount++;
                cache[id] = value = default;
                return false;
            }
        }


        #endregion Public Methods
    }
}
