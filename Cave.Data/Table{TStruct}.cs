using System;

namespace Cave.Data
{
    /// <summary>Provides a table of structures (rows).</summary>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public class Table<TStruct> : AbstractTable<TStruct>
        where TStruct : struct
    {
        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="Table{TStruct}" /> class.</summary>
        /// <param name="table">The table instance to wrap.</param>
        public Table(ITable table)
        {
            BaseTable = table ?? throw new ArgumentNullException(nameof(table));
            if (table.Flags.HasFlag(TableFlags.IgnoreMissingFields))
            {
                var layout = RowLayout.CreateTyped(typeof(TStruct));
                Layout = layout.GetMatching(BaseTable.Layout, table.Flags);
            }
            else
            {
                Layout = RowLayout.CreateTyped(typeof(TStruct));
                RowLayout.CheckLayout(Layout, BaseTable.Layout);
            }

            BaseTable.UseLayout(Layout);
        }

        #endregion

        #region AbstractTable<TStruct> Overrides

        /// <inheritdoc />
        protected override ITable BaseTable { get; }

        /// <inheritdoc />
        public override void Connect(IDatabase database, TableFlags flags, RowLayout layout) => BaseTable.Connect(database, flags, layout);

        /// <inheritdoc />
        public override RowLayout Layout { get; }

        /// <summary>Not supported.</summary>
        /// <param name="layout">Unused parameter.</param>
        public override void UseLayout(RowLayout layout) => RowLayout.CheckLayout(Layout, layout);

        #endregion
    }
}
