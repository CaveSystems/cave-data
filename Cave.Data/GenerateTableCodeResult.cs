using System;

namespace Cave.Data
{
    /// <summary>Provides the code generated as result of a ITable.GenerateX() function.</summary>
    public struct GenerateTableCodeResult : IEquatable<GenerateTableCodeResult>
    {
        #region Public Properties

        /// <summary>Gets the class name of the generated table structure.</summary>
        public string ClassName { get; internal set; }

        /// <summary>Gets the generated code.</summary>
        public string Code { get; internal set; }

        /// <summary>Gets the name of the database.</summary>
        public string DatabaseName { get; internal set; }

        /// <summary>Gets the filename the code was saved to. This may be null if code was not saved to a file.</summary>
        public string FileName { get; internal set; }

        /// <summary>Gets the table name used as getter at the database class.</summary>
        public string GetterName { get; internal set; }

        /// <summary>Gets the table name at the database.</summary>
        public string TableName { get; internal set; }

        #endregion Public Properties

        #region Public Methods

        /// <summary>The inequality operator != returns true if its operands are not equal, false otherwise.</summary>
        /// <param name="left">First operand.</param>
        /// <param name="right">Second operand.</param>
        /// <returns>true if its operands are not equal, false otherwise.</returns>
        public static bool operator !=(GenerateTableCodeResult left, GenerateTableCodeResult right) => !Equals(left, right);

        /// <summary>The equality operator == returns true if its operands are equal, false otherwise.</summary>
        /// <param name="left">First operand.</param>
        /// <param name="right">Second operand.</param>
        /// <returns>true if its operands are equal, false otherwise.</returns>
        public static bool operator ==(GenerateTableCodeResult left, GenerateTableCodeResult right) => Equals(left, right);

        /// <inheritdoc/>
        public bool Equals(GenerateTableCodeResult other) =>
            (DatabaseName == other.DatabaseName) && (TableName == other.TableName) && (Code == other.Code) &&
            (FileName == other.FileName) && (ClassName == other.ClassName);

        /// <inheritdoc/>
        public override bool Equals(object obj) => obj is GenerateTableCodeResult other && Equals(other);

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = DatabaseName != null ? DatabaseName.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (TableName != null ? TableName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Code != null ? Code.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FileName != null ? FileName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ClassName != null ? ClassName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (GetterName != null ? GetterName.GetHashCode() : 0);
                return hashCode;
            }
        }

        #endregion Public Methods
    }
}
