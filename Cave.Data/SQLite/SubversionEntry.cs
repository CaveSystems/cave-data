namespace Cave.Data.SQLite
{
    /// <summary>Provides access to a subversion entry.</summary>
    public class SubversionEntry
    {
        readonly string[] data;

        #region Constructors

        /// <summary>Initializes a new instance of the <see cref="SubversionEntry" /> class.</summary>
        /// <param name="data">Lines.</param>
        /// <param name="version">Version code.</param>
        internal SubversionEntry(string[] data, int version)
        {
            Version = version;
            this.data = data;
            IsValid = (Version >= 8) || (Version <= 10);
        }

        #endregion

        #region Properties

        /// <summary>Gets a value indicating whether the entry was deleted or not.</summary>
        public bool Deleted
        {
            get
            {
                switch (Version)
                {
                    case 8:
                    case 9:
                    case 10:
                        return data[5] == "delete";
                    default:
                        return false;
                }
            }
        }

        /// <summary>Gets a value indicating whether the entry is valid or not.</summary>
        public bool IsValid { get; }

        /// <summary>Gets the name of the <see cref="SubversionEntry" />.</summary>
        public string Name => data[0];

        /// <summary>Gets the type of the <see cref="SubversionEntry" />.</summary>
        public SubversionEntryType Type
        {
            get
            {
                switch (data[1])
                {
                    case "dir": return SubversionEntryType.Directory;
                    case "file": return SubversionEntryType.File;
                    default: return SubversionEntryType.Unknown;
                }
            }
        }

        /// <summary>Gets the subversion entry version.</summary>
        public int Version { get; }

        #endregion

        #region Overrides

        /// <inheritdoc />
        public override int GetHashCode() => ToString().GetHashCode();

        /// <inheritdoc />
        public override string ToString() => Name + " " + Type;

        #endregion
    }
}
