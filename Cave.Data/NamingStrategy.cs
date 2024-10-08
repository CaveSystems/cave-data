using System;

namespace Cave.Data;

/// <summary>Provides strategies to create names from text.</summary>
public enum NamingStrategy
{
    /// <summary>Keep exact name.</summary>
    Exact,

    /// <summary>Build aLowerCamelCaseName.</summary>
    CamelCase,

    /// <summary>Build a_snake_case_name.</summary>
    SnakeCase,

    /// <summary>Build a-kebab-case-name.</summary>
    KebabCase,

    /// <summary>Build APascalCaseName.</summary>
    PascalCase,
}
