using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a memory based storage engine for databases, tables and rows.
    /// </summary>
    public sealed class MemoryStorage : Storage
    {
        #region Private Fields

        readonly Dictionary<string, IDatabase> Databases = new();

        #endregion Private Fields

        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryStorage"/> class.
        /// </summary>
        /// <param name="options">Options for the databaseName.</param>
        public MemoryStorage(ConnectionFlags options = ConnectionFlags.None)
            : base("memory://", options)
        {
        }

        #endregion Public Constructors

        #region Public Properties

        /// <summary>
        /// Gets the default memory storage.
        /// </summary>
        /// <value>The default memory storage.</value>
        public static MemoryStorage Default { get; } = new MemoryStorage();

        /// <inheritdoc/>
        public override IList<string> DatabaseNames
        {
            get
            {
                if (Closed)
                {
                    throw new ObjectDisposedException(ToString());
                }

                return Databases.Keys.ToArray();
            }
        }

        /// <inheritdoc/>
        public override bool SupportsNativeTransactions { get; }

        #endregion Public Properties

        #region Public Methods

        /// <inheritdoc/>
        public override IDatabase CreateDatabase(string databaseName)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (HasDatabase(databaseName))
            {
                throw new InvalidOperationException($"Database '{databaseName}' already exists!");
            }

            IDatabase database = new MemoryDatabase(this, databaseName);
            Databases.Add(databaseName, database);
            return database;
        }

        /// <inheritdoc/>
        public override void DeleteDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (!Databases.Remove(database))
            {
                throw new ArgumentException($"The requested databaseName '{database}' was not found!");
            }
        }

        /// <inheritdoc/>
        public override IDatabase GetDatabase(string databaseName)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (!HasDatabase(databaseName))
            {
                throw new ArgumentException($"The requested databaseName '{databaseName}' was not found!");
            }

            return Databases[databaseName];
        }

        /// <inheritdoc/>
        public override bool HasDatabase(string databaseName) => Databases.ContainsKey(databaseName);

        #endregion Public Methods
    }
}
