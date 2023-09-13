using System;

namespace Cave.Data
{
    /// <summary>
    /// Transaction flags.
    /// </summary>
    [Flags]
    public enum TransactionFlags
    {
        /// <summary>
        /// No settings
        /// </summary>
        None = 0,

        /// <summary>
        /// Do not use. Default is to throw exceptions.
        /// </summary>
        [Obsolete("Default action is throw exceptions. Use flag TransactionFlags.NoExceptions to change this behaviour.")]
        ThrowExceptions = 1 << 0,

        /// <summary>
        /// Do not throw exceptions
        /// </summary>
        NoExceptions = 1 << 1
    }
}
