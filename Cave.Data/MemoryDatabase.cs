using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>Provides a memory based database.</summary>
    public sealed class MemoryDatabase : Database
    {
        #region Static

        /// <summary>Gets the default memory database.</summary>
        /// <value>The default memory database.</value>
        public static MemoryDatabase Default { get; } = new MemoryDatabase();

        #endregion

        Dictionary<string, ITable> tables = new Dictionary<string, ITable>();

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="MemoryDatabase" /> class.</summary>
        /// <param name="storage">The storage engine.</param>
        /// <param name="name">The name of the database.</param>
        public MemoryDatabase(MemoryStorage storage, string name)
            : base(storage, name)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="MemoryDatabase" /> class.</summary>
        public MemoryDatabase()
            : base(MemoryStorage.Default, "Default")
        {
        }

        #endregion

        #region Overrides

        /// <inheritdoc />
        public override void Close()
        {
            if (IsClosed)
            {
                throw new ObjectDisposedException(Name);
            }

            tables.Clear();
            tables = null;
        }

        /// <inheritdoc />
        public override ITable CreateTable(RowLayout layout, TableFlags flags = default)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            if (tables.ContainsKey(layout.Name))
            {
                throw new InvalidOperationException($"Table '{layout.Name}' already exists!");
            }

            var table = MemoryTable.Create(layout);
            tables[layout.Name] = table;
            return table;
        }

        /// <inheritdoc />
        public override void DeleteTable(string tableName)
        {
            if (!tables.Remove(tableName))
            {
                throw new ArgumentException($"Table '{tableName}' does not exist!");
            }
        }

        /// <inheritdoc />
        public override ITable GetTable(string tableName, TableFlags flags = default) => tables[tableName];

        /// <inheritdoc />
        protected override string[] GetTableNames() => !IsClosed ? tables.Keys.ToArray() : throw new ObjectDisposedException(Name);

        /// <inheritdoc />
        public override bool IsClosed => tables == null;

        /// <inheritdoc />
        public override bool IsSecure => true;

        #endregion
    }
}
