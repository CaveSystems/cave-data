using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides a table field index implementation.</summary>
public sealed class FieldIndex : IFieldIndex
{
    #region Private Fields

    /// <summary>resolves value to IDs.</summary>
    readonly FakeSortedDictionary<List<object?[]>> index;

    readonly object nullValue = new();

    #endregion Private Fields

    #region Private Methods

    static int GetRowIndex(IList<object?[]> rows, object?[] row)
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

    #endregion Private Methods

    #region Internal Methods

    /// <summary>Adds an ID, object combination to the index.</summary>
    internal void Add(object?[] row, int fieldNumber)
    {
        var value = row.GetValue(fieldNumber);
        var obj = value ?? nullValue;
        if (index.ContainsKey(obj))
        {
            index[obj].Add(row);
        }
        else
        {
            index[obj] = [row];
        }

        Count++;
    }

    /// <summary>Clears the index.</summary>
    internal void Clear()
    {
        index.Clear();
        Count = 0;
    }

    /// <summary>Removes a row from the index.</summary>
    /// <param name="row">The row.</param>
    /// <param name="fieldNumber">The fieldnumber.</param>
    /// <exception cref="ArgumentException">
    /// Value {value} is not present at index (equals check {index})! or Row {row} is not present at index! (Present: {value} =&gt; {rows})! or Could not remove
    /// row {row} value {value}!.
    /// </exception>
    internal void Delete(object?[] row, int fieldNumber)
    {
        var value = row.GetValue(fieldNumber);
        var obj = value ?? nullValue;

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

    /// <summary>Replaces a row at the index.</summary>
    /// <param name="oldRow">Row to remove.</param>
    /// <param name="newRow">Row to add.</param>
    /// <param name="fieldNumber">Fieldnumber.</param>
    /// <exception cref="ArgumentException">
    /// Value {value} is not present at index (equals check {index})! or Row {row} is not present at index! (Present: {value} =&gt; {rows})! or Could not remove
    /// row {row} value {value}!.
    /// </exception>
    internal void Replace(object?[] oldRow, object?[] newRow, int fieldNumber)
    {
        if (Equals(oldRow, newRow))
        {
            return;
        }

        Delete(oldRow, fieldNumber);
        Add(newRow, fieldNumber);
    }

    #endregion Internal Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="FieldIndex"/> class.</summary>
    public FieldIndex() => index = new();

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the id count.</summary>
    /// <value>The id count.</value>
    public int Count { get; private set; }

    #endregion Public Properties

    #region Public Methods

    /// <summary>Obtains all IDs with the specified hashcode.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The rows.</returns>
    public IEnumerable<object?[]> Find(object? value)
    {
        var obj = value ?? nullValue;
        return index.ContainsKey(obj) ? index[obj].ToArray() : [];
    }

    #endregion Public Methods
}
