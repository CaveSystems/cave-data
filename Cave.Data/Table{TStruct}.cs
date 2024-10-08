using System;

namespace Cave.Data;

/// <summary>Provides a table of structures (rows).</summary>
/// <typeparam name="TStruct">Row structure type.</typeparam>
public class Table<TStruct> : AbstractTable<TStruct>
    where TStruct : struct
{
    #region Protected Properties

    /// <inheritdoc/>
    protected override ITable BaseTable { get; }

    #endregion Protected Properties

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="Table{TStruct}"/> class.</summary>
    /// <param name="table">The table instance to wrap.</param>
    public Table(ITable table)
    {
        BaseTable = table ?? throw new ArgumentNullException(nameof(table));
        if (table.Flags.HasFlag(TableFlags.IgnoreMissingFields))
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct), nameOverride: table.Name);
            Layout = layout.GetMatching(BaseTable.Layout, table.Flags);
        }
        else
        {
            Layout = RowLayout.CreateTyped(typeof(TStruct), nameOverride: table.Name);
            RowLayout.CheckLayout(Layout, BaseTable.Layout);
        }

        BaseTable.UseLayout(Layout);
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
    public override void UseLayout(RowLayout layout) => RowLayout.CheckLayout(Layout, layout);

    #endregion Public Methods
}
