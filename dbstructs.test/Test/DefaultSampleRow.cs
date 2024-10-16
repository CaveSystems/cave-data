//-----------------------------------------------------------------------
// <summary>
// Autogenerated table class
// </summary>
// <auto-generated />
//-----------------------------------------------------------------------

#nullable enable

using System;
using System.ComponentModel;
using System.Globalization;
using System.CodeDom.Compiler;
using Cave;
using Cave.Collections;
using Cave.Data;
using Cave.IO;

namespace dbstructs.test
{
    /// <summary>Table structure for sample.</summary>
    [GeneratedCode("Cave.Data.TableInterfaceGenerator", null)]
    [Table("sample")]
    public partial struct DefaultSampleRow : IEquatable<DefaultSampleRow>
    {
        /// <summary>Converts the string representation of a row to its DefaultSampleRow equivalent.</summary>
        /// <param name="data">A string that contains a row to convert.</param>
        /// <returns>A new DefaultSampleRow instance.</returns>
        public static DefaultSampleRow Parse(string data) => Parse(data, CultureInfo.InvariantCulture);

        /// <summary>Converts the string representation of a row to its DefaultSampleRow equivalent.</summary>
        /// <param name="data">A string that contains a row to convert.</param>
        /// <param name="provider">The format provider (optional).</param>
        /// <returns>A new DefaultSampleRow instance.</returns>
        public static DefaultSampleRow Parse(string data, IFormatProvider provider) => CsvReader.ParseRow<DefaultSampleRow>(data, provider);

        /// <summary>[ID, AutoIncrement] long Id</summary>
        [Field(Flags = FieldFlags.ID | FieldFlags.AutoIncrement, Length = 0, AlternativeNames = "")]
        public long Id;

        /// <summary>byte[] Blob</summary>
        [Field(Length = 0, AlternativeNames = "")]
        public byte[] Blob;

        /// <summary>byte Byte</summary>
        [Field(Length = 0, AlternativeNames = "")]
        public byte Byte;

        /// <summary>TimeSpan Native Duration</summary>
        [Field(Length = 0, AlternativeNames = "")]
        [DateTimeFormat(DateTimeKind.Unspecified, DateTimeType.Native)]
        public TimeSpan Duration;

        /// <summary>bool Enabled (4)</summary>
        [Field(Length = 4, AlternativeNames = "")]
        public bool Enabled;

        /// <summary>Guid UTF8 Guid</summary>
        [Field(Length = 0, AlternativeNames = "")]
        [StringFormat(StringEncoding.UTF8)]
        public Guid Guid;

        /// <summary>sbyte SByte</summary>
        [Field(Name = "SByte", Length = 0, AlternativeNames = "")]
        public sbyte Sbyte;

        /// <summary>UTF8 UTF8 Text (30)</summary>
        [Field(Length = 30, AlternativeNames = "")]
        [StringFormat(StringEncoding.UTF8)]
        public UTF8 Text;

        /// <summary>DateTime Native Timestamp</summary>
        [Field(Length = 0, AlternativeNames = "")]
        [DateTimeFormat(DateTimeKind.Unspecified, DateTimeType.Native)]
        public DateTime Timestamp;

        /// <summary>int UserId</summary>
        [Field(Length = 0, AlternativeNames = "")]
        public int UserId;

        /// <summary>Gets a string representation of this row.</summary>
        /// <returns>Returns a string that can be parsed by <see cref="Parse(string)"/>.</returns>
        public override string ToString() => ToString(CultureInfo.InvariantCulture);

        /// <summary>Gets a string representation of this row.</summary>
        /// <returns>Returns a string that can be parsed by <see cref="Parse(string, IFormatProvider)"/>.</returns>
        public string ToString(IFormatProvider provider) => CsvWriter.RowToString(this, provider);

        /// <summary>Gets the hash code based on all fields of this row.</summary>
        /// <returns>A hash code for all fields of this row.</returns>
        public override int GetHashCode()
        {
            var hasher = DefaultHashingFunction.Create();
            hasher.Add(Id);
            hasher.Add(Blob);
            hasher.Add(Byte);
            hasher.Add(Duration);
            hasher.Add(Enabled);
            hasher.Add(Guid);
            hasher.Add(Sbyte);
            hasher.Add(Text);
            hasher.Add(Timestamp);
            hasher.Add(UserId);
            return hasher.ToHashCode();
        }

        /// <inheritdoc/>
        public override bool Equals(object? other) => other is DefaultSampleRow row && Equals(row);

        /// <inheritdoc/>
        public bool Equals(DefaultSampleRow other)
        {
            return
                Equals(other.Id, Id) &&
                DefaultComparer.Equals(other.Blob, Blob) &&
                Equals(other.Byte, Byte) &&
                Equals(other.Duration, Duration) &&
                Equals(other.Enabled, Enabled) &&
                Equals(other.Guid, Guid) &&
                Equals(other.Sbyte, Sbyte) &&
                Equals(other.Text, Text) &&
                Equals(other.Timestamp, Timestamp) &&
                Equals(other.UserId, UserId);
        }
    }
}
