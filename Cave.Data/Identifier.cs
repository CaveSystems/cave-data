using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides a database identifier (alias primary key).</summary>
public class Identifier : IEquatable<Identifier>
{
    #region Private Fields

    readonly object?[] data;

    #endregion Private Fields

    #region Private Methods

    static object?[] GetData(Row row, IEnumerable<int> fields)
    {
        var result = fields.Select(fieldsIndex => row[fieldsIndex]).ToArray();
        if (result.Length == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fields));
        }

        return result;
    }

    #endregion Private Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="Identifier"/> class.</summary>
    /// <param name="row">Row to create to create identifier for.</param>
    /// <param name="layout">Table layout.</param>
    public Identifier(Row row, RowLayout layout)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        if (!layout.Identifier.Any())
        {
            data = row.Values;
        }
        else
        {
            data = layout.Identifier.Select(field => row[field.Index]).ToArray();
        }
    }

    /// <summary>Initializes a new instance of the <see cref="Identifier"/> class.</summary>
    /// <param name="row">Row to create to create identifier for.</param>
    /// <param name="fields">The fields to use for the identifier.</param>
    public Identifier(Row row, params int[] fields) => data = GetData(row, fields);

    /// <summary>Initializes a new instance of the <see cref="Identifier"/> class.</summary>
    /// <param name="row">Row to create to create identifier for.</param>
    /// <param name="fields">The fields to use for the identifier.</param>
    public Identifier(Row row, IEnumerable<int> fields) => data = GetData(row, fields);

    #endregion Public Constructors

    #region Public Methods

    /// <inheritdoc/>
    public bool Equals(Identifier? other) => (other is not null) && data.SequenceEqual(other.data);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Identifier other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var hash = 0;
        foreach (var item in data)
        {
            hash ^= item?.GetHashCode() ?? 0;
            hash = hash.BitwiseRotateLeft(1);
        }

        return hash;
    }

    /// <inheritdoc/>
    public override string ToString() => data.Select(d => $"{d}").Join('|');

    #endregion Public Methods
}
