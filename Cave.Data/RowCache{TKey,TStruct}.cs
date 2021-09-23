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

        readonly Dictionary<TKey, TStruct?> Cache = new();
        readonly ITable<TKey, TStruct> Table;

        #endregion Private Fields

        #region Public Constructors

        /// <summary>
        /// Creates a new row cache using the specified table.
        /// </summary>
        /// <param name="table">Table to read rows from.</param>
        public RowCache(ITable table) => this.Table = new Table<TKey, TStruct>(table);

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
                Cache.Clear();
            }

            ;
        }

        /// <summary>
        /// Gets the row with the specified identifier.
        /// </summary>
        /// <param name="id">Row identifier.</param>
        /// <returns>Returns the row structure or null.</returns>
        public TStruct? Get(TKey id)
        {
            lock (this)
            {
                if (Cache.TryGetValue(id, out var result))
                {
                    HitCount++;
                    return result;
                }

                MissCount++;
                if (Table.TryGetStruct(id, out var value))
                {
                    return Cache[id] = value;
                }
            }

            NotFoundCount++;
            return Cache[id] = null;
        }

        /// <summary>
        /// Tries to get the row with the specified identifier.
        /// </summary>
        /// <param name="id">The identifier value.</param>
        /// <param name="row">Returns the result row.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        public bool TryGetStruct(TKey id, out TStruct row)
        {
            var result = Get(id);
            row = result ?? default;
            return result.HasValue;
        }

        #endregion Public Methods
    }
}
