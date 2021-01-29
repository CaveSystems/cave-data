using System;

namespace Cave.Data.Sql
{
    /// <summary>Provides a <see cref="IDatabase" /> implementation for sql92 databases.</summary>
    public abstract class SqlDatabase : Database
    {
        bool closed;

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="SqlDatabase" /> class.</summary>
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

        #endregion

        #region Properties

        /// <summary>Gets the underlying SqlStorage engine.</summary>
        protected SqlStorage SqlStorage { get; }

        #endregion

        #region Overrides

        /// <summary>Gets a value indicating whether this instance was closed.</summary>
        public override bool IsClosed => closed;

        #endregion

        #region IDatabase Member

        /// <inheritdoc />
        public override void DeleteTable(string tableName) =>
            SqlStorage.Execute(database: Name, table: tableName, cmd: "DROP TABLE " + SqlStorage.FQTN(Name, tableName));

        /// <inheritdoc />
        public override void Close()
        {
            if (IsClosed)
            {
                throw new ObjectDisposedException(Name);
            }

            closed = true;
        }

        #endregion
    }
}
