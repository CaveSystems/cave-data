using System;
using Cave.IO;

namespace Cave.Data;

/// <summary>Provides an <see cref="Attribute"/> for configuring string/varchar/text fields.</summary>
[AttributeUsage(AttributeTargets.Field)]
public sealed class StringFormatAttribute : Attribute
{
    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="StringFormatAttribute"/> class.</summary>
    /// <param name="encoding">String encoding to use.</param>
    public StringFormatAttribute(StringEncoding encoding) => Encoding = encoding;

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the <see cref="StringEncoding"/>.</summary>
    public StringEncoding Encoding { get; }

    #endregion Public Properties
}
