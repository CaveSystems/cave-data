using System;
using System.Collections.Generic;

namespace Cave.Data;

/// <summary>Provides a row cache class working with row structures.</summary>
/// <typeparam name="TKey">Key identifier type.</typeparam>
/// <typeparam name="TStruct">Row structure type.</typeparam>
/// <typeparam name="TTarget">Result and cache item type.</typeparam>
public class RowCache<TKey, TStruct, TTarget> : IRowCache where TKey : IComparable<TKey> where TStruct : struct where TTarget : class
{
    #region Private Fields

    readonly Dictionary<TKey, TTarget> cache = new();

    readonly RowCacheConvertFunction converterFunction;

    readonly ITable<TKey, TStruct> table;

    #endregion Private Fields

    #region Public Constructors

    /// <summary>Creates a new row cache using the specified table.</summary>
    /// <param name="table">Table to read rows from.</param>
    /// <param name="func">Function to convert from <typeparamref name="TStruct"/> to <typeparamref name="TTarget"/>.</param>
    public RowCache(ITable table, RowCacheConvertFunction func)
    {
        this.table = new Table<TKey, TStruct>(table);
        converterFunction = func ?? throw new ArgumentNullException(nameof(func));
    }

    #endregion Public Constructors

    #region Public Delegates

    /// <summary>Converts a <typeparamref name="TStruct"/> row instance to the result <typeparamref name="TTarget"/> value.</summary>
    /// <param name="id">Identifier of the dataset.</param>
    /// <param name="row">Structure row read from table. This may be equal to null if the dataset was not found at the table.</param>
    /// <returns>Returns a new <typeparamref name="TTarget"/> instance or null.</returns>
    public delegate TTarget RowCacheConvertFunction(TKey id, TStruct? row);

    #endregion Public Delegates

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

    /// <summary>Gets the row with the specified identifier.</summary>
    /// <param name="id">Row identifier.</param>
    /// <returns>Returns the converted target value or null.</returns>
    public TTarget? Get(TKey id) => TryGetValue(id, out var value) ? value : value;

    /// <summary>Tries to get the value with the specified identifier.</summary>
    /// <param name="id">The identifier value.</param>
    /// <param name="value">Returns the result value.</param>
    /// <returns>Returns true on success, false otherwise.</returns>
    public bool TryGetValue(TKey id, out TTarget? value)
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
                cache[id] = value = converterFunction(id, row);
                return true;
            }

            NotFoundCount++;
            cache[id] = value = converterFunction(id, null);
            return false;
        }
    }

    #endregion Public Methods
}
