using System.Collections.Generic;

namespace Cave.Data;

/// <summary>Provides extensions to the <see cref="IStorage"/> interface.</summary>
public static class IStorageExtensions
{
    #region Public Methods

    /// <summary>Creates an <see cref="IEnumerator{IDatabase}"/> for the specified <paramref name="storage"/>.</summary>
    /// <param name="storage">Storage to iterate</param>
    /// <returns>Returns a new <see cref="IEnumerator{IDatabase}"/> instance</returns>
    public static IEnumerator<IDatabase> GetTableEnumerator(this IStorage storage)
    {
        foreach (var databaseName in storage.DatabaseNames)
        {
            yield return storage.GetDatabase(databaseName);
        }
    }

    /// <summary>Creates an <see cref="IEnumerable{IDatabase}"/> for the specified <paramref name="storage"/>.</summary>
    /// <param name="storage">Storage to iterate</param>
    /// <returns>Returns a new <see cref="IEnumerable{IDatabase}"/> instance</returns>
    public static IEnumerable<IDatabase> GetTables(this IStorage storage)
    {
        foreach (var databaseName in storage.DatabaseNames)
        {
            yield return storage.GetDatabase(databaseName);
        }
    }

    #endregion Public Methods
}
