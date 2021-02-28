using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    class FakeSortedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        #region Private Fields

        readonly IDictionary<TKey, TValue> Unsorted;
        TKey[] sortedKeys;

        #endregion Private Fields

        #region Public Constructors

        public FakeSortedDictionary() => Unsorted = new Dictionary<TKey, TValue>();

        public FakeSortedDictionary(int capacity) => Unsorted = new Dictionary<TKey, TValue>(capacity);

        #endregion Public Constructors

        #region Public Properties

        public IList<TKey> SortedKeys
        {
            get
            {
                if (sortedKeys == null)
                {
                    sortedKeys = new TKey[Unsorted.Count];
                    Unsorted.Keys.CopyTo(sortedKeys, 0);
                    Array.Sort(sortedKeys);
                }

                return sortedKeys;
            }
        }

        public ICollection<TKey> UnsortedKeys => Unsorted.Keys;
        public IList<TValue> Values => SortedKeys.Select(k => Unsorted[k]).ToList();

        public int Count => Unsorted.Count;

        public bool IsReadOnly => Unsorted.IsReadOnly;

        #endregion Public Properties

        #region Public Indexers

        public TValue this[TKey key]
        {
            get => Unsorted[key];
            set
            {
                Unsorted[key] = value;
                sortedKeys = null;
            }
        }

        #endregion Public Indexers

        #region Public Methods

        public void Add(TKey key, TValue value)
        {
            Unsorted.Add(key, value);
            sortedKeys = null;
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Unsorted.Add(item);
            sortedKeys = null;
        }

        public void Clear()
        {
            Unsorted.Clear();
            sortedKeys = null;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) => Unsorted.Contains(item);

        public bool ContainsKey(TKey key) => Unsorted.ContainsKey(key);

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) => Unsorted.CopyTo(array, arrayIndex);

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => ToArray().GetEnumerator();

        public bool Remove(TKey key)
        {
            sortedKeys = null;
            return Unsorted.Remove(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            sortedKeys = null;
            return Unsorted.Remove(item);
        }

        public IList<KeyValuePair<TKey, TValue>> ToArray()
        {
            var count = Unsorted.Count;
            var result = new KeyValuePair<TKey, TValue>[count];
            var i = 0;
            foreach (var key in SortedKeys)
            {
                result[i++] = new KeyValuePair<TKey, TValue>(key, Unsorted[key]);
            }

            return result;
        }

        public bool TryGetValue(TKey key, out TValue value) => Unsorted.TryGetValue(key, out value);

        IEnumerator IEnumerable.GetEnumerator() => ToArray().GetEnumerator();

        #endregion Public Methods
    }
}
