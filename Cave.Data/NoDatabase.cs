using System;

namespace Cave.Data;

sealed class NoDatabase : Database
{
    internal NoDatabase() : base(Cave.Data.Storage.None, string.Empty) { }
    public override bool IsClosed => true;
    public override bool IsSecure => false;
    public override void Close() { }
    public override ITable CreateTable(RowLayout layout, TableFlags flags = TableFlags.None) => throw new NotSupportedException();
    public override void DeleteTable(string tableName) => throw new NotSupportedException();
    public override ITable GetTable(string tableName, TableFlags flags = TableFlags.None) => throw new NotSupportedException();
    protected override string[] GetTableNames() => throw new NotSupportedException();
}
