using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides an interface for Database storage functions.</summary>
    public interface IStorage
    {
        /// <summary>Gets or sets a value indicating whether [log verbose messages].</summary>
        /// <value><c>true</c> if [log verbose messages]; otherwise, <c>false</c>.</value>
        bool LogVerboseMessages { get; set; }

        /// <summary>Gets a value indicating whether [unsafe connections are allowed].</summary>
        /// <value><c>true</c> if [unsafe connections are allowed]; otherwise, <c>false</c>.</value>
        bool AllowUnsafeConnections { get; }

        /// <summary>
        ///     Gets a value indicating whether the storage engine supports native transactions with faster execution than
        ///     single commands.
        /// </summary>
        /// <value><c>true</c> if supports native transactions; otherwise, <c>false</c>.</value>
        bool SupportsNativeTransactions { get; }

        /// <summary>Gets the date time format for big int date time values.</summary>
        string BigIntDateTimeFormat { get; }

        /// <summary>Gets or sets number of rows per chunk on big data operations.</summary>
        int TransactionRowCount { get; set; }

        /// <summary>Gets the connection string used to connect to the storage engine.</summary>
        ConnectionString ConnectionString { get; }

        /// <summary>Gets a value indicating whether the storage was already closed or not.</summary>
        bool Closed { get; }

        /// <summary>Gets all available databaseName names.</summary>
        IList<string> DatabaseNames { get; }

        /// <summary>Gets the maximum <see cref="float" /> precision at the value of 1.0f of this storage engine.</summary>
        float FloatPrecision { get; }

        /// <summary>Gets the maximum <see cref="double" /> precision at the value of 1.0d of this storage engine.</summary>
        double DoublePrecision { get; }

        /// <summary>Gets the maximum <see cref="DateTime" /> value precision (absolute) of this storage engine.</summary>
        TimeSpan DateTimePrecision { get; }

        /// <summary>Gets the maximum <see cref="TimeSpan" /> value precision (absolute) of this storage engine.</summary>
        TimeSpan TimeSpanPrecision { get; }

        /// <summary>Gets the databaseName with the specified name.</summary>
        /// <param name="database">The name of the databaseName.</param>
        /// <returns>A new <see cref="IDatabase" /> instance for the requested databaseName.</returns>
        IDatabase this[string database] { get; }

        /// <summary>closes the connection to the storage engine.</summary>
        void Close();

        /// <summary>Checks whether the databaseName with the specified name exists at the databaseName or not.</summary>
        /// <param name="databaseName">The name of the databaseName.</param>
        /// <returns>True if the databaseName exists, false otherwise.</returns>
        bool HasDatabase(string databaseName);

        /// <summary>Gets the databaseName with the specified name.</summary>
        /// <param name="databaseName">The name of the databaseName.</param>
        /// <returns>A new <see cref="IDatabase" /> instance for the requested databaseName.</returns>
        IDatabase GetDatabase(string databaseName);

        /// <summary>Gets the databaseName with the specified name.</summary>
        /// <param name="databaseName">The name of the databaseName.</param>
        /// <param name="createIfNotExists">Create the databaseName if its not already present.</param>
        /// <returns>A new <see cref="IDatabase" /> instance for the requested databaseName.</returns>
        IDatabase GetDatabase(string databaseName, bool createIfNotExists);

        /// <summary>Adds a new databaseName with the specified name.</summary>
        /// <param name="databaseName">The name of the databaseName.</param>
        /// <returns>A new <see cref="IDatabase" /> instance for the created databaseName.</returns>
        IDatabase CreateDatabase(string databaseName);

        /// <summary>Removes the specified databaseName.</summary>
        /// <param name="database">The name of the databaseName.</param>
        void DeleteDatabase(string database);

        /// <summary>Gets converted field properties for this storage instance based on requested field properties.</summary>
        /// <param name="field">The field properties to convert.</param>
        /// <returns>A new <see cref="FieldProperties" /> instance.</returns>
        IFieldProperties GetDatabaseFieldProperties(IFieldProperties field);

        /// <summary>Gets the maximum <see cref="decimal" /> value precision (absolute) for the specified field length.</summary>
        /// <param name="count">The length (0 = default).</param>
        /// <returns>The precision at the databaseName.</returns>
        decimal GetDecimalPrecision(float count);

        /// <summary>
        ///     Checks two layouts for equality using the databaseName field type conversion and throws an error if the layouts do
        ///     not match.
        /// </summary>
        /// <param name="expected">The expected layout.</param>
        /// <param name="current">The layout to check.</param>
        void CheckLayout(RowLayout expected, RowLayout current);
    }
}
