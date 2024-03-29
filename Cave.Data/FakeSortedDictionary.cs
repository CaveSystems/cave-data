﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    class FakeSortedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        #region Private Fields

        readonly IDictionary<TKey, TValue> unsorted;
        TKey[] sortedKeys;

        #endregion Private Fields

        #region Public Constructors

        public FakeSortedDictionary() => unsorted = new Dictionary<TKey, TValue>();

        public FakeSortedDictionary(int capacity) => unsorted = new Dictionary<TKey, TValue>(capacity);

        #endregion Public Constructors

        #region Public Properties

        public IList<TKey> SortedKeys
        {
            get
            {
                if (sortedKeys == null)
                {
                    sortedKeys = new TKey[unsorted.Count];
                    unsorted.Keys.CopyTo(sortedKeys, 0);
                    Array.Sort(sortedKeys);
                }

                return sortedKeys;
            }
        }

        public ICollection<TKey> UnsortedKeys => unsorted.Keys;
        public IList<TValue> Values => SortedKeys.Select(k => unsorted[k]).ToList();

        public int Count => unsorted.Count;

        public bool IsReadOnly => unsorted.IsReadOnly;

        #endregion Public Properties

        #region Public Indexers

        public TValue this[TKey key]
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

        public void Add(TKey key, TValue value)
        {
            unsorted.Add(key, value);
            sortedKeys = null;
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            unsorted.Add(item);
            sortedKeys = null;
        }

        public void Clear()
        {
            unsorted.Clear();
            sortedKeys = null;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) => unsorted.Contains(item);

        public bool ContainsKey(TKey key) => unsorted.ContainsKey(key);

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) => unsorted.CopyTo(array, arrayIndex);

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => ToArray().GetEnumerator();

        public bool Remove(TKey key)
        {
            sortedKeys = null;
            return unsorted.Remove(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            sortedKeys = null;
            return unsorted.Remove(item);
        }

        public IList<KeyValuePair<TKey, TValue>> ToArray()
        {
            var count = unsorted.Count;
            var result = new KeyValuePair<TKey, TValue>[count];
            var i = 0;
            foreach (var key in SortedKeys)
            {
                result[i++] = new KeyValuePair<TKey, TValue>(key, unsorted[key]);
            }

            return result;
        }

        public bool TryGetValue(TKey key, out TValue value) => unsorted.TryGetValue(key, out value);

        IEnumerator IEnumerable.GetEnumerator() => ToArray().GetEnumerator();

        #endregion Public Methods
    }
}
