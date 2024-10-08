using System.Collections.Generic;

namespace Cave.Data;

/// <summary>Provides a table field index implementation.</summary>
public interface IFieldIndex
{
    #region Public Methods

    /// <summary>Retrieves all identifiers for the specified value.</summary>
    /// <param name="value">The value.</param>
    /// <returns>All matching rows found.</returns>
    IEnumerable<object?[]> Find(object? value);

    #endregion Public Methods
}
