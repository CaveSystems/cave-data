using System;
using System.Diagnostics.CodeAnalysis;

namespace Cave.Data
{
    /// <summary>Transaction flags.</summary>
    [Flags]
    [SuppressMessage("Naming", "CA1711")]
    [SuppressMessage("Usage", "CA2217")]
    public enum TransactionFlags
    {
        /// <summary>No settings</summary>
        None = 0,

        /// <summary>Do not use. Default is to throw exceptions.</summary>
        [Obsolete("Default action is throw exceptions. Use flag TransactionFlags.NoExceptions to change this behaviour.")]
        ThrowExceptions = 1 << 0,

        /// <summary>Do not throw exceptions</summary>
        NoExceptions = 1 << 1,
    }
}
