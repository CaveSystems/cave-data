using System;

namespace Cave.Data;

/// <summary>Provides field visibility</summary>
[Flags]
public enum FieldVisibility
{
    /// <summary>Field shall be private</summary>
    Private = 0,

    /// <summary>Field shall be protected (can be combined with <see cref="Internal"/>)</summary>
    Protected = 1,

    /// <summary>Field shall be internal (can be combined with <see cref="Protected"/>)</summary>
    Internal = 2,

    /// <summary>Field shall be public</summary>
    Public = 4,
}
