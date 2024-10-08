using System;
using System.Collections;
using System.Collections.Generic;

namespace Cave.Data;

sealed class TableSorter : IComparer<Row>
{
    #region Private Fields

    readonly IComparer comparer;
    readonly bool descending;
    readonly IFieldProperties field;

    #endregion Private Fields

    #region Public Constructors

    public TableSorter(IFieldProperties field, ResultOptionMode mode)
    {
        this.field = field ?? throw new ArgumentNullException(nameof(field));
        comparer = field.DataType switch
        {
            DataType.Bool => Comparer<bool>.Default,
            DataType.Int8 => Comparer<sbyte>.Default,
            DataType.Int16 => Comparer<short>.Default,
            DataType.Int32 => Comparer<int>.Default,
            DataType.Int64 => Comparer<long>.Default,
            DataType.UInt8 => Comparer<byte>.Default,
            DataType.UInt16 => Comparer<ushort>.Default,
            DataType.UInt32 => Comparer<uint>.Default,
            DataType.UInt64 => Comparer<ulong>.Default,
            DataType.Char => Comparer<char>.Default,
            DataType.DateTime => Comparer<DateTime>.Default,
            DataType.Decimal => Comparer<decimal>.Default,
            DataType.Double => Comparer<double>.Default,
            DataType.Enum => System.Collections.Comparer.Default,
            DataType.Single => Comparer<float>.Default,
            DataType.String => Comparer<string>.Default,
            DataType.TimeSpan => Comparer<TimeSpan>.Default,
            DataType.User => Comparer<string>.Default,
            _ => throw new NotSupportedException(),
        };
        descending = mode switch
        {
            ResultOptionMode.SortAsc => false,
            ResultOptionMode.SortDesc => true,
            _ => throw new ArgumentOutOfRangeException(nameof(mode)),
        };
    }

    #endregion Public Constructors

    #region Public Methods

    public int Compare(Row? row1, Row? row2)
    {
        if (row1 is null) return row2 is null ? 0 : -1;
        if (row2 is null) return 1;
        var val1 = row1[field.Index];
        var val2 = row2[field.Index];
        return descending ? comparer.Compare(val2, val1) : comparer.Compare(val1, val2);
    }

    #endregion Public Methods
}
