using System;
using System.Globalization;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides a table of structures (rows).</summary>
/// <typeparam name="TKey">Key identifier type.</typeparam>
/// <typeparam name="TStruct">Row structure type.</typeparam>
public class Table<TKey, TStruct> : AbstractTable<TKey, TStruct>
    where TKey : IComparable<TKey>
    where TStruct : struct
{
    #region Protected Properties

    /// <inheritdoc/>
    protected override ITable BaseTable { get; }

    /// <inheritdoc/>
    protected override IFieldProperties KeyField { get; }

    #endregion Protected Properties

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="Table{TKey, TStruct}"/> class.</summary>
    /// <param name="table">The table instance to wrap.</param>
    public Table(ITable table)
    {
        BaseTable = table ?? throw new ArgumentNullException(nameof(table));

        var tableLayout = table.Layout;
        var structLayout = RowLayout.CreateTyped(typeof(TStruct));
        if (table.Flags.HasFlag(TableFlags.IgnoreMissingFields))
        {
            tableLayout = structLayout.GetMatching(table.Layout, table.Flags);
        }
        else
        {
            RowLayout.CheckLayout(structLayout, tableLayout);
            tableLayout = structLayout;
        }

        var keyField = tableLayout.Identifier.Single();
        var dbValue = (IConvertible)Activator.CreateInstance(keyField.ValueType);
        var converted = (IConvertible)dbValue.ToType(typeof(TKey), CultureInfo.InvariantCulture);
        var test = (IConvertible)converted.ToType(keyField.ValueType, CultureInfo.InvariantCulture);
        if (!Equals(test, dbValue))
        {
            throw new ArgumentException($"Type (local) {nameof(TKey)} can not be converted from and to (database) {keyField.ValueType}!");
        }

        KeyField = keyField;
        table.UseLayout(tableLayout);
        Layout = tableLayout;
    }

    #endregion Public Constructors

    #region Public Properties

    /// <inheritdoc/>
    public override RowLayout Layout { get; }

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override void Connect(IDatabase database, TableFlags flags, RowLayout layout) => BaseTable.Connect(database, flags, layout);

    /// <summary>Not supported.</summary>
    /// <param name="layout">Unused parameter.</param>
    public override void UseLayout(RowLayout layout) => throw new NotSupportedException();

    #endregion Public Methods
}
