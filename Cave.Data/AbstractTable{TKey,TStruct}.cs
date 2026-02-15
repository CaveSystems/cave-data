using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides a table of structures (rows).</summary>
/// <typeparam name="TKey">Key identifier type.</typeparam>
/// <typeparam name="TStruct">Row structure type.</typeparam>
public abstract class AbstractTable<TKey, TStruct> : AbstractTable<TStruct>, ITable<TKey, TStruct>, ITable<TStruct>, ITable
    where TKey : IComparable<TKey>
    where TStruct : struct
{
    #region Protected Properties

    /// <summary>Gets the identifier field.</summary>
    protected abstract IFieldProperties KeyField { get; }

    #endregion Protected Properties

    #region Public Indexers

    /// <inheritdoc/>
    public TStruct this[TKey id] => GetStruct(id);

    #endregion Public Indexers

    #region Public Methods

    /// <inheritdoc/>
    public IList<TKey> GetKeys() => BaseTable.GetValues<TKey>(KeyField.Name, null);

    /// <inheritdoc/>
    public void Delete(TKey id)
    {
        var result = BaseTable.TryDelete(KeyField.Name, id);
        switch (result)
        {
            case 0: throw new ArgumentException($"Could not delete id {id}. No matching dataset found!");
            case 1: break;
            default: throw new InvalidOperationException($"Multiple datasets with id {id} deleted!");
        }
    }

    /// <inheritdoc/>
    public void Delete(IEnumerable<TKey> ids) => BaseTable.TryDelete(Search.FieldIn(KeyField.Name, ids));

    /// <inheritdoc/>
    public bool Exist(TKey id) => BaseTable.Exist(KeyField.Name, id);

    /// <inheritdoc/>
    public IDictionary<TKey, TStruct> GetDictionary(Search? search = null, ResultOption? resultOption = null) =>
        GetRows(search, resultOption).ToDictionary(r => (TKey)r[KeyField.Index]!, r => r.GetStruct<TStruct>(Layout));

    /// <inheritdoc/>
    public Row GetRow(TKey id) => BaseTable.GetRow(KeyField.Name, id);

    /// <inheritdoc/>
    public IList<Row> GetRows(IEnumerable<TKey> ids) => BaseTable.GetRows(Search.FieldIn(KeyField.Name, ids));

    /// <inheritdoc/>
    public TStruct GetStruct(TKey id) => GetRow(id).GetStruct<TStruct>(Layout);

    /// <inheritdoc/>
    public IList<TStruct> GetStructs(IEnumerable<TKey> ids) =>
        GetRows(Search.FieldIn(KeyField.Name, ids)).Select(r => r.GetStruct<TStruct>(Layout)).ToList();
    
    /// <summary>
    /// Returns an enumeration of all structures at the database.
    /// The identifiers are fetched at creation, changes (especially deletions) at the database after creation will result in skipped structures.
    /// </summary>
    /// <returns>Returns a new <see cref="IEnumerator{T}"/> instance with the current present identifiers.</returns>
    public IEnumerator<TStruct> GetEnumerator()
    {
        foreach (var key in GetKeys())
        {
            if (this.TryGetStruct(KeyField.Name, key, out var result))
            {
                yield return result;
            }
        }
    }

    /// <summary>
    /// Returns an enumeration of all structures at the database.
    /// The identifiers are fetched at creation, changes (especially deletions) at the database after creation will result in skipped structures.
    /// </summary>
    /// <returns>Returns a new <see cref="IEnumerator{T}"/> instance with the current present identifiers.</returns>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    #endregion Public Methods
}
