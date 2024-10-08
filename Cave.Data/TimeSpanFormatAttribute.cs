using System;

namespace Cave.Data;

/// <summary>Provides an <see cref="Attribute"/> for configuring timespan fields.</summary>
[AttributeUsage(AttributeTargets.Field)]
public sealed class TimeSpanFormatAttribute : Attribute
{
    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="TimeSpanFormatAttribute"/> class.</summary>
    /// <param name="type"><see cref="DateTimeType"/>.</param>
    public TimeSpanFormatAttribute(DateTimeType type) => Type = type;

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the <see cref="DateTimeType"/>.</summary>
    public DateTimeType Type { get; }

    #endregion Public Properties
}
