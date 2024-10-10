using System;
using System.Collections.Generic;
using System.Data;
using Cave.Data.Sql;

namespace Cave.Data;

sealed class NoStorage : SqlStorage
{
    #region Protected Methods

    protected override string GetConnectionString(string database) => throw new NotImplementedException();

    protected override IDbConnection GetDbConnectionType() => throw new NotImplementedException();

    #endregion Protected Methods

    #region Protected Internal Properties

    protected internal override bool DBConnectionCanChangeDataBase => false;

    #endregion Protected Internal Properties

    #region Public Constructors

    public NoStorage() : base(new(), 0) { }

    #endregion Public Constructors

    #region Public Properties

    public override IList<string> DatabaseNames => [];
    public override string ParameterPrefix => string.Empty;
    public override bool SupportsAllFieldsGroupBy => false;
    public override bool SupportsNamedParameters => false;
    public override bool SupportsNativeTransactions => false;

    #endregion Public Properties

    #region Public Methods

    public override IDatabase CreateDatabase(string databaseName) => throw new NotSupportedException();

    public override void DeleteDatabase(string database) => throw new NotSupportedException();

    public override string EscapeFieldName(IFieldProperties field) => throw new NotImplementedException();

    public override string FQTN(string database, string table) => throw new NotImplementedException();

    public override IDatabase GetDatabase(string databaseName) => throw new NotSupportedException();

    public override bool HasDatabase(string databaseName) => throw new NotSupportedException();

    #endregion Public Methods
}
