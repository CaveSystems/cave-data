using System;

namespace Cave.Data;

/// <summary>Provides extensions to the <see cref="NamingStrategy"/> enumeration.</summary>
public static class NamingStrategyExtension
{
    #region Public Methods

    /// <summary>Builds a new name matching the the specified <paramref name="namingStrategy"/>.</summary>
    /// <param name="namingStrategy">Naming strategy to use.</param>
    /// <param name="name">Original name.</param>
    /// <returns>A new string containing the new name.</returns>
    public static string GetNameByStrategy(this NamingStrategy namingStrategy, string name)
    {
        if (name == null)
        {
            throw new ArgumentNullException(nameof(name));
        }

        return namingStrategy switch
        {
            NamingStrategy.Exact => name,
            NamingStrategy.CamelCase => name.GetCamelCaseName(),
            NamingStrategy.SnakeCase => name.GetSnakeCaseName(),
            NamingStrategy.PascalCase => name.GetPascalCaseName(),
            NamingStrategy.KebabCase => name.GetKebabCaseName(),
            _ => throw new NotImplementedException()
        };
    }

    #endregion Public Methods
}
