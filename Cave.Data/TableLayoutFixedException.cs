using System;
using System.Runtime.Serialization;

namespace Cave.Data
{
    /// <summary>The table layout is already fixed and can no longer be changed !.</summary>
    public class TableLayoutFixedException : Exception
    {
        #region Public Constructors

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        public TableLayoutFixedException()
            : base("The table layout is already fixed and can no longer be changed!")
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="msg">The message.</param>
        public TableLayoutFixedException(string msg)
            : base(msg)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="msg">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public TableLayoutFixedException(string msg, Exception innerException)
            : base(msg, innerException)
        {
        }

        #endregion Public Constructors
    }
}
