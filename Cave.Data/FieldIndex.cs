using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table field index implementation.
    /// </summary>
    public sealed class FieldIndex : IFieldIndex
    {
#if USE_BOXING
        /// <summary>
        /// resolves value to IDs
        /// </summary>
        SortedDictionary<BoxedValue, Set<string>> index;
#else

        /// <summary>
        /// resolves value to IDs.
        /// </summary>
        readonly FakeSortedDictionary<object, List<object[]>> index;

        readonly object nullValue = new BoxedValue(null);
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="FieldIndex"/> class.
        /// </summary>
        public FieldIndex() =>
#if USE_BOXING
            index = new FakeSortedDictionary<BoxedValue, List<object[]>>();
#else
            index = new FakeSortedDictionary<object, List<object[]>>();

#endif

        /// <summary>
        /// Gets the id count.
        /// </summary>
        /// <value>The id count.</value>
        public int Count { get; private set; }

        /// <summary>
        /// Obtains all IDs with the specified hashcode.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>The rows.</returns>
        public IEnumerable<object[]> Find(object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value ?? nullValue;
#endif
            return index.ContainsKey(obj) ? index[obj].ToArray() : new object[][] { };
        }

        /// <summary>
        /// Clears the index.
        /// </summary>
        internal void Clear()
        {
            index.Clear();
            Count = 0;
        }

        /// <summary>
        /// Adds an ID, object combination to the index.
        /// </summary>
        internal void Add(object[] row, int fieldNumber)
        {
            var value = row.GetValue(fieldNumber);
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value ?? nullValue;
#endif
            if (index.ContainsKey(obj))
            {
                index[obj].Add(row);
            }
            else
            {
                index[obj] = new List<object[]> { row };
            }

            Count++;
        }

        /// <summary>
        /// Replaces a row at the index.
        /// </summary>
        /// <param name="oldRow">Row to remove.</param>
        /// <param name="newRow">Row to add.</param>
        /// <param name="fieldNumber">Fieldnumber.</param>
        /// <exception cref="ArgumentException">
        /// Value {value} is not present at index (equals check {index})! or Row {row} is not present at index! (Present: {value} =&gt; {rows})! or Could not
        /// remove row {row} value {value}!.
        /// </exception>
        internal void Replace(object[] oldRow, object[] newRow, int fieldNumber)
        {
            if (Equals(oldRow, newRow))
            {
                return;
            }

            Delete(oldRow, fieldNumber);
            Add(newRow, fieldNumber);
        }

        /// <summary>
        /// Removes a row from the index.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="fieldNumber">The fieldnumber.</param>
        /// <exception cref="ArgumentException">
        /// Value {value} is not present at index (equals check {index})! or Row {row} is not present at index! (Present: {value} =&gt; {rows})! or Could not
        /// remove row {row} value {value}!.
        /// </exception>
        internal void Delete(object[] row, int fieldNumber)
        {
            var value = row.GetValue(fieldNumber);
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value ?? nullValue;
#endif

            // remove ID from old hash
            if (!index.TryGetValue(obj, out var rows))
            {
                throw new ArgumentException($"Value {value} is not present at index (equals check {index.Join(",")})!");
            }

            var i = GetRowIndex(rows, row);
            if (i < 0)
            {
                throw new KeyNotFoundException($"Row {row} is not present at index! (Present: {value} => {rows.Join(",")})!");
            }

            if (rows.Count > 1)
            {
                rows.RemoveAt(i);
            }
            else
            {
                if (!index.Remove(obj))
                {
                    throw new ArgumentException($"Could not remove row {row} value {value}!");
                }
            }

            Count--;
        }

        static int GetRowIndex(IList<object[]> rows, object[] row)
        {
            for (var i = 0; i < rows.Count; i++)
            {
                if (rows[i].SequenceEqual(row))
                {
                    return i;
                }
            }

            return -1;
        }

        class BoxedValue : IComparable<BoxedValue>, IEquatable<BoxedValue>, IComparable
        {
            readonly object value;

            #region Constructors

            public BoxedValue(object value) => this.value = value;

            #endregion Constructors

            #region IComparable Members

            public int CompareTo(object obj) => obj is BoxedValue box ? CompareTo(box) : Comparer.Default.Compare(value, obj);

            #endregion IComparable Members

            #region IComparable<BoxedValue> Members

            public int CompareTo(BoxedValue other) => Comparer.Default.Compare(value, other.value);

            #endregion IComparable<BoxedValue> Members

            #region IEquatable<BoxedValue> Members

            public bool Equals(BoxedValue other) => Equals(other.value, value);

            #endregion IEquatable<BoxedValue> Members

            #region Overrides

            public override bool Equals(object obj) => obj is BoxedValue box ? Equals(box) : Equals(obj, value);

            public override int GetHashCode() => value == null ? 0 : value.GetHashCode();

            public override string ToString() => value == null ? "<null>" : value.ToString();

            #endregion Overrides
        }
    }
}
