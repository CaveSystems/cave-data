using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data;

/// <summary>Provides a table of structures (rows).</summary>
/// <typeparam name="TStruct">Row structure type.</typeparam>
public abstract class AbstractTable<TStruct> : ITable<TStruct>, ITable
    where TStruct : struct
{
    #region Protected Properties

    /// <summary>Gets the base table used by this instance.</summary>
    protected abstract ITable BaseTable { get; }

    #endregion Protected Properties

    #region Public Properties

    /// <inheritdoc/>
    public IDatabase Database => BaseTable.Database;

    /// <inheritdoc/>
    public virtual TableFlags Flags => BaseTable.Flags;

    /// <inheritdoc/>
    public abstract RowLayout Layout { get; }

    /// <inheritdoc/>
    public string Name => BaseTable.Name;

    /// <inheritdoc/>
    public long RowCount => BaseTable.RowCount;

    /// <inheritdoc/>
    public int SequenceNumber => BaseTable.SequenceNumber;

    /// <inheritdoc/>
    public IStorage Storage => BaseTable.Storage;

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public void Clear() => BaseTable.Clear();

    /// <inheritdoc/>
    public int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = default) => BaseTable.Commit(transactions, flags);

    /// <inheritdoc/>
    public abstract void Connect(IDatabase database, TableFlags flags, RowLayout layout);

    /// <inheritdoc/>
    public long Count(Search? search = null, ResultOption? resultOption = null) => BaseTable.Count(search, resultOption);

    /// <inheritdoc/>
    public void Delete(Row row) => BaseTable.Delete(row);

    /// <inheritdoc/>
    public void Delete(IEnumerable<Row> rows) => BaseTable.Delete(rows);

    /// <inheritdoc/>
    public void Delete(TStruct row) => BaseTable.Delete(Layout.GetRow(row));

    /// <inheritdoc/>
    public void Delete(IEnumerable<TStruct> rows) => BaseTable.Delete(rows.Select(r => Layout.GetRow(r)));

    /// <inheritdoc/>
    public IList<TValue> Distinct<TValue>(string fieldName, Search? search = null)
        where TValue : IComparable =>
        BaseTable.Distinct<TValue>(fieldName, search);

    /// <inheritdoc/>
    public bool Exist(Search? search) => BaseTable.Exist(search);

    /// <inheritdoc/>
    public bool Exist(Row row) => BaseTable.Exist(row);

    /// <inheritdoc/>
    public bool Exist(TStruct row) => BaseTable.Exist(Layout.GetRow(row));

    /// <inheritdoc/>
    public Row GetRow(Search? search = null, ResultOption? resultOption = null) => BaseTable.GetRow(search, resultOption);

    /// <inheritdoc/>
    public Row GetRow(TStruct row) => BaseTable.GetRow(Search.IdentifierMatch(BaseTable, Layout.GetRow(row)));

    /// <inheritdoc/>
    public Row GetRowAt(int index) => BaseTable.GetRowAt(index);

    /// <inheritdoc/>
    public IList<Row> GetRows() => BaseTable.GetRows();

    /// <inheritdoc/>
    public IList<Row> GetRows(Search? search = null, ResultOption? resultOption = null) => BaseTable.GetRows(search, resultOption);

    /// <inheritdoc/>
    public IList<Row> GetRows(IEnumerable<TStruct> rows)
    {
        if (rows == null)
        {
            throw new ArgumentNullException(nameof(rows));
        }

        var search = Search.None;
        foreach (var row in rows)
        {
            search |= Search.IdentifierMatch(BaseTable, Layout.GetRow(row));
        }

        return BaseTable.GetRows(search);
    }

    /// <inheritdoc/>
    public TStruct GetStruct(TStruct row) => GetRow(row).GetStruct<TStruct>(Layout);

    /// <inheritdoc/>
    public TStruct GetStruct(Search? search = null, ResultOption? resultOption = null) => GetRow(search, resultOption).GetStruct<TStruct>(Layout);

    /// <inheritdoc/>
    public TStruct GetStructAt(int index) => GetRowAt(index).GetStruct<TStruct>(Layout);

    /// <inheritdoc/>
    public IList<TStruct> GetStructs(Search? search = null, ResultOption? resultOption = null) =>
        GetRows(search, resultOption).Select(r => r.GetStruct<TStruct>(Layout)).ToList();

    /// <inheritdoc/>
    public IList<TStruct> GetStructs(IEnumerable<TStruct> rows) => GetRows(rows).Select(r => r.GetStruct<TStruct>(Layout)).ToList();

    /// <inheritdoc/>
    public IList<TValue> GetValues<TValue>(string fieldName, Search? search)
        where TValue : IComparable<TValue> =>
        BaseTable.GetValues<TValue>(fieldName, search);

    /// <inheritdoc/>
    public IList<TValue> GetValues<TValue>(string fieldName, Search? search = null, ResultOption? resultOption = null)
        where TValue : IComparable<TValue> =>
        BaseTable.GetValues<TValue>(fieldName, search, resultOption);

    /// <inheritdoc/>
    public Row Insert(Row row) => BaseTable.Insert(row);

    /// <inheritdoc/>
    public void Insert(IEnumerable<Row> rows) => BaseTable.Insert(rows);

    /// <inheritdoc/>
    public TStruct Insert(TStruct row) => Insert(Layout.GetRow(row)).GetStruct<TStruct>(Layout);

    /// <inheritdoc/>
    public void Insert(IEnumerable<TStruct> rows) => Insert(rows.Select(r => Layout.GetRow(r)));

    /// <inheritdoc/>
    public TValue? Maximum<TValue>(string fieldName, Search? search = null)
        where TValue : struct, IComparable =>
        BaseTable.Maximum<TValue>(fieldName, search);

    /// <inheritdoc/>
    public TValue? Minimum<TValue>(string fieldName, Search? search = null)
        where TValue : struct, IComparable =>
        BaseTable.Minimum<TValue>(fieldName, search);

    /// <inheritdoc/>
    public void Replace(Row row) => BaseTable.Replace(row);

    /// <inheritdoc/>
    public void Replace(IEnumerable<Row> rows) => BaseTable.Replace(rows);

    /// <inheritdoc/>
    public void Replace(TStruct row) => Replace(Layout.GetRow(row));

    /// <inheritdoc/>
    public void Replace(IEnumerable<TStruct> rows) => Replace(rows.Select(r => Layout.GetRow(r)));

    /// <inheritdoc/>
    public void SetValue(string fieldName, object value) => BaseTable.SetValue(fieldName, value);

    /// <inheritdoc/>
    public double Sum(string fieldName, Search? search = null) => BaseTable.Sum(fieldName, search);

    /// <inheritdoc/>
    public int TryDelete(Search? search) => BaseTable.TryDelete(search);

    /// <inheritdoc/>
    public void Update(Row row) => BaseTable.Update(row);

    /// <inheritdoc/>
    public void Update(IEnumerable<Row> rows) => BaseTable.Update(rows);

    /// <inheritdoc/>
    public void Update(TStruct row) => Update(Layout.GetRow(row));

    /// <inheritdoc/>
    public void Update(IEnumerable<TStruct> rows) => Update(rows.Select(r => Layout.GetRow(r)));

    /// <inheritdoc/>
    public abstract void UseLayout(RowLayout layout);

    #endregion Public Methods
}
