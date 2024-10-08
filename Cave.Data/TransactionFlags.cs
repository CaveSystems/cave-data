using System;

namespace Cave.Data;

/// <summary>Transaction flags.</summary>
[Flags]
public enum TransactionFlags
{
    /// <summary>No settings</summary>
    None = 0,

    /// <summary>Do not throw exceptions</summary>
    NoExceptions = 1 << 1
}
