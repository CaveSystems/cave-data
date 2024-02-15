using System;

namespace Cave.Data
{
    /// <summary>Provides strategies to create names from text.</summary>
    public enum NamingStrategy
    {
        /// <summary>Keep exact name.</summary>
        Exact,

        /// <summary>Build aLowerCamelCaseName.</summary>
        [Obsolete("Use LowerCamelCase or PascalCase instead.")]
        CamelCase,

        /// <summary>Build a_snake_case_name.</summary>
        SnakeCase,

        /// <summary>Build a-kebab-case-name.</summary>
        KebabCase,

        /// <summary>Build APascalCaseName.</summary>
        PascalCase,

        /// <summary>Build aLowerCamelCaseName.</summary>
        LowerCamelCase,
    }
}
