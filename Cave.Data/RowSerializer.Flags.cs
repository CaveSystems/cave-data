using System;
using System.Diagnostics.CodeAnalysis;

namespace Cave.Data
{
    /// <summary>Provides Row based serialization.</summary>
    public static partial class RowSerializer
    {
        #region Flags enum

        /// <summary>Settings used during de/serialization.</summary>
        [Flags]
        public enum Flags
        {
            /// <summary>No flags</summary>
            None = 0,

            /// <summary>Serialize the layout first, then the data. This adds type safety to the stream but costs a lot of bandwidth and time.</summary>
            WithLayout = 1
        }

        #endregion
    }
}
