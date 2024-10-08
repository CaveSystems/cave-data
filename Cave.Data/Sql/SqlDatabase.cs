using System;

namespace Cave.Data.Sql;

/// <summary>Provides a <see cref="IDatabase"/> implementation for sql92 databases.</summary>
public abstract class SqlDatabase : Database
{
    #region Private Fields

    bool closed;

    #endregion Private Fields

    #region Protected Constructors

    /// <summary>Initializes a new instance of the <see cref="SqlDatabase"/> class.</summary>
    /// <param name="storage">The storage engine the database belongs to.</param>
    /// <param name="name">The name of the database.</param>
    protected SqlDatabase(SqlStorage storage, string name)
        : base(storage, name)
    {
        SqlStorage = storage;
        if (name.HasInvalidChars(ASCII.Strings.SafeName))
        {
            throw new ArgumentException("Name contains invalid chars!");
        }
    }

    #endregion Protected Constructors

    #region Protected Properties

    /// <summary>Gets the underlying SqlStorage engine.</summary>
    protected SqlStorage SqlStorage { get; }

    #endregion Protected Properties

    #region Public Properties

    /// <summary>Gets a value indicating whether this instance was closed.</summary>
    public override bool IsClosed => closed;

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override void Close()
    {
        if (IsClosed)
        {
            throw new ObjectDisposedException(Name);
        }

        closed = true;
    }

    /// <inheritdoc/>
    public override void DeleteTable(string tableName) =>
        SqlStorage.Execute(database: Name, table: tableName, cmd: "DROP TABLE " + SqlStorage.FQTN(Name, tableName));

    #endregion Public Methods
}
