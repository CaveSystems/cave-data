using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides a row cache class working with row structures.</summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    /// <typeparam name="TTarget">Result and cache item type.</typeparam>
    public class RowCache<TKey, TStruct, TTarget> : IRowCache where TKey : IComparable<TKey> where TStruct : struct where TTarget : class
    {
        #region Nested type: RowCacheConvertFunction

        /// <summary>Converts a <typeparamref name="TStruct" /> row instance to the result <typeparamref name="TTarget" /> value.</summary>
        /// <param name="id">Identifier of the dataset.</param>
        /// <param name="row">Structure row read from table. This may be equal to null if the dataset was not found at the table.</param>
        /// <returns>Returns a new <typeparamref name="TTarget" /> instance or null.</returns>
        public delegate TTarget RowCacheConvertFunction(TKey id, TStruct? row);

        #endregion

        readonly Dictionary<TKey, TTarget> cache = new Dictionary<TKey, TTarget>();

        readonly RowCacheConvertFunction func;
        readonly ITable<TKey, TStruct> table;

        #region Constructors

        /// <summary>Creates a new row cache using the specified table.</summary>
        /// <param name="table">Table to read rows from.</param>
        /// <param name="func">Function to convert from <typeparamref name="TStruct" /> to <typeparamref name="TTarget" />.</param>
        public RowCache(ITable table, RowCacheConvertFunction func)
        {
            this.table = new Table<TKey, TStruct>(table);
            this.func = func ?? throw new ArgumentNullException(nameof(func));
        }

        #endregion

        #region IRowCache Members

        /// <inheritdoc />
        public void Clear()
        {
            lock (this)
            {
                HitCount = 0;
                MissCount = 0;
                NotFoundCount = 0;
                cache.Clear();
            }

            ;
        }

        /// <inheritdoc />
        public long HitCount { get; set; }

        /// <inheritdoc />
        public long MissCount { get; set; }

        /// <inheritdoc />
        public long NotFoundCount { get; set; }

        #endregion

        #region Members

        /// <summary>Gets the row with the specified identifier.</summary>
        /// <param name="id">Row identifier.</param>
        /// <returns>Returns the converted target value or null.</returns>
        public TTarget Get(TKey id)
        {
            TryGetValue(id, out var value);
            return value;
        }

        /// <summary>Tries to get the value with the specified identifier.</summary>
        /// <param name="id">The identifier value.</param>
        /// <param name="value">Returns the result value.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        public bool TryGetValue(TKey id, out TTarget value)
        {
            lock (this)
            {
                if (cache.TryGetValue(id, out value))
                {
                    HitCount++;
                    return true;
                }

                MissCount++;
                if (table.TryGetStruct(id, out var row))
                {
                    value = cache[id] = func(id, row);
                    return true;
                }
            }

            NotFoundCount++;
            value = cache[id] = func(id, null);
            return false;
        }

        #endregion
    }
}
