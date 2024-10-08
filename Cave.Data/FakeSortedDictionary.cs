using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Cave.Data;

sealed class FakeSortedDictionary<TValue> : IEnumerable<KeyValuePair<object, TValue>>
{
    #region Private Fields

    readonly IDictionary<object, TValue> unsorted;
    object[]? sortedKeys;

    #endregion Private Fields

    #region Public Constructors

    public FakeSortedDictionary() => unsorted = new Dictionary<object, TValue>();

    public FakeSortedDictionary(int capacity) => unsorted = new Dictionary<object, TValue>(capacity);

    #endregion Public Constructors

    #region Public Properties

    public int Count => unsorted.Count;

    public bool IsReadOnly => unsorted.IsReadOnly;

    public IList<object> SortedKeys
    {
        get
        {
            if (sortedKeys == null)
            {
                sortedKeys = new object[unsorted.Count];
                unsorted.Keys.CopyTo(sortedKeys, 0);
                Array.Sort(sortedKeys);
            }

            return sortedKeys;
        }
    }

    public ICollection<object> UnsortedKeys => unsorted.Keys;
    public IList<TValue> Values => SortedKeys.Select(k => unsorted[k]).ToList();

    #endregion Public Properties

    #region Public Indexers

    public TValue this[object key]
    {
        get => unsorted[key];
        set
        {
            unsorted[key] = value;
            sortedKeys = null;
        }
    }

    #endregion Public Indexers

    #region Public Methods

    public void Add(object key, TValue value)
    {
        unsorted.Add(key, value);
        sortedKeys = null;
    }

    public void Add(KeyValuePair<object, TValue> item)
    {
        unsorted.Add(item);
        sortedKeys = null;
    }

    public void Clear()
    {
        unsorted.Clear();
        sortedKeys = null;
    }

    public bool Contains(KeyValuePair<object, TValue> item) => unsorted.Contains(item);

    public bool ContainsKey(object key) => unsorted.ContainsKey(key);

    public void CopyTo(KeyValuePair<object, TValue>[] array, int arrayIndex) => unsorted.CopyTo(array, arrayIndex);

    public IEnumerator<KeyValuePair<object, TValue>> GetEnumerator() => ToArray().GetEnumerator();

    public bool Remove(object key)
    {
        sortedKeys = null;
        return unsorted.Remove(key);
    }

    public bool Remove(KeyValuePair<object, TValue> item)
    {
        sortedKeys = null;
        return unsorted.Remove(item);
    }

    public IList<KeyValuePair<object, TValue>> ToArray()
    {
        var count = unsorted.Count;
        var result = new KeyValuePair<object, TValue>[count];
        var i = 0;
        foreach (var key in SortedKeys)
        {
            result[i++] = new KeyValuePair<object, TValue>(key, unsorted[key]);
        }

        return result;
    }

    public bool TryGetValue(object key, [MaybeNullWhen(false)] out TValue value) => unsorted.TryGetValue(key, out value);

    IEnumerator IEnumerable.GetEnumerator() => ToArray().GetEnumerator();

    #endregion Public Methods
}
