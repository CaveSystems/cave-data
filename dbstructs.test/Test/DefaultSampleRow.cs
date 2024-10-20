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
        /// <remarks>Id field</remarks>
        [Description("Id field")]
        [Field(Flags = FieldFlags.ID | FieldFlags.AutoIncrement)]
        public long Id;

        /// <summary>byte[] Blob</summary>
        [Field()]
        public byte[] Blob;

        /// <summary>byte Byte</summary>
        [Field()]
        public byte Byte;

        /// <summary>short Short</summary>
        [Field()]
        public short Short;

        /// <summary>int Int</summary>
        [Field()]
        public int Int;

        /// <summary>long Long</summary>
        [Field()]
        public long Long;

        /// <summary>TimeSpan Native Duration</summary>
        [Field()]
        [DateTimeFormat(DateTimeKind.Unspecified, DateTimeType.Native)]
        public TimeSpan Duration;

        /// <summary>bool Enabled (4)</summary>
        [Field(Length = 4)]
        public bool Enabled;

        /// <summary>Guid UTF8 Guid</summary>
        [Field()]
        [StringFormat(StringEncoding.UTF8)]
        public Guid Guid;

        /// <summary>sbyte SByte</summary>
        [Field(Name = "SByte")]
        public sbyte Sbyte;

        /// <summary>ushort UShort</summary>
        [Field(Name = "UShort")]
        public ushort Ushort;

        /// <summary>uint UInt</summary>
        [Field(Name = "UInt")]
        public uint Uint;

        /// <summary>ulong ULong</summary>
        [Field(Name = "ULong")]
        public ulong Ulong;

        /// <summary>UTF8 UTF8 Text (30)</summary>
        [Field(Length = 30)]
        [StringFormat(StringEncoding.UTF8)]
        public UTF8 Text;

        /// <summary>string UTF8 String</summary>
        [Field()]
        [StringFormat(StringEncoding.UTF8)]
        public string String;

        /// <summary>DateTime Native Timestamp</summary>
        [Field()]
        [DateTimeFormat(DateTimeKind.Unspecified, DateTimeType.Native)]
        public DateTime Timestamp;

        /// <summary>Uri UTF8 Uri</summary>
        [Field()]
        [StringFormat(StringEncoding.UTF8)]
        public Uri Uri;

        /// <summary>byte[] NullableBlob</summary>
        [Field()]
        public byte[] NullableBlob;

        /// <summary>byte NullableByte</summary>
        [Field()]
        public byte NullableByte;

        /// <summary>short NullableShort</summary>
        [Field()]
        public short NullableShort;

        /// <summary>int NullableInt</summary>
        [Field()]
        public int NullableInt;

        /// <summary>long NullableLong</summary>
        [Field()]
        public long NullableLong;

        /// <summary>TimeSpan Native NullableDuration</summary>
        [Field()]
        [DateTimeFormat(DateTimeKind.Unspecified, DateTimeType.Native)]
        public TimeSpan NullableDuration;

        /// <summary>bool NullableEnabled</summary>
        [Field()]
        public bool NullableEnabled;

        /// <summary>Guid UTF8 NullableGuid</summary>
        [Field()]
        [StringFormat(StringEncoding.UTF8)]
        public Guid NullableGuid;

        /// <summary>sbyte NullableSByte</summary>
        [Field(Name = "NullableSByte")]
        public sbyte NullableSbyte;

        /// <summary>ushort NullableUShort</summary>
        [Field(Name = "NullableUShort")]
        public ushort NullableUshort;

        /// <summary>uint NullableUInt</summary>
        [Field(Name = "NullableUInt")]
        public uint NullableUint;

        /// <summary>ulong NullableULong</summary>
        [Field(Name = "NullableULong")]
        public ulong NullableUlong;

        /// <summary>UTF8 UTF8 NullableText (30)</summary>
        [Field(Length = 30)]
        [StringFormat(StringEncoding.UTF8)]
        public UTF8 NullableText;

        /// <summary>DateTime Native NullableTimestamp</summary>
        [Field()]
        [DateTimeFormat(DateTimeKind.Unspecified, DateTimeType.Native)]
        public DateTime NullableTimestamp;

        /// <summary>string UTF8 NullableString</summary>
        [Field()]
        [StringFormat(StringEncoding.UTF8)]
        public string NullableString;

        /// <summary>Uri UTF8 NullableUri</summary>
        [Field()]
        [StringFormat(StringEncoding.UTF8)]
        public Uri NullableUri;

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
            hasher.Add(Short);
            hasher.Add(Int);
            hasher.Add(Long);
            hasher.Add(Duration);
            hasher.Add(Enabled);
            hasher.Add(Guid);
            hasher.Add(Sbyte);
            hasher.Add(Ushort);
            hasher.Add(Uint);
            hasher.Add(Ulong);
            hasher.Add(Text);
            hasher.Add(String);
            hasher.Add(Timestamp);
            hasher.Add(Uri);
            hasher.Add(NullableBlob);
            hasher.Add(NullableByte);
            hasher.Add(NullableShort);
            hasher.Add(NullableInt);
            hasher.Add(NullableLong);
            hasher.Add(NullableDuration);
            hasher.Add(NullableEnabled);
            hasher.Add(NullableGuid);
            hasher.Add(NullableSbyte);
            hasher.Add(NullableUshort);
            hasher.Add(NullableUint);
            hasher.Add(NullableUlong);
            hasher.Add(NullableText);
            hasher.Add(NullableTimestamp);
            hasher.Add(NullableString);
            hasher.Add(NullableUri);
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
                Equals(other.Short, Short) &&
                Equals(other.Int, Int) &&
                Equals(other.Long, Long) &&
                Equals(other.Duration, Duration) &&
                Equals(other.Enabled, Enabled) &&
                Equals(other.Guid, Guid) &&
                Equals(other.Sbyte, Sbyte) &&
                Equals(other.Ushort, Ushort) &&
                Equals(other.Uint, Uint) &&
                Equals(other.Ulong, Ulong) &&
                Equals(other.Text, Text) &&
                DefaultComparer.Equals(other.String, String) &&
                Equals(other.Timestamp, Timestamp) &&
                Equals(other.Uri, Uri) &&
                DefaultComparer.Equals(other.NullableBlob, NullableBlob) &&
                Equals(other.NullableByte, NullableByte) &&
                Equals(other.NullableShort, NullableShort) &&
                Equals(other.NullableInt, NullableInt) &&
                Equals(other.NullableLong, NullableLong) &&
                Equals(other.NullableDuration, NullableDuration) &&
                Equals(other.NullableEnabled, NullableEnabled) &&
                Equals(other.NullableGuid, NullableGuid) &&
                Equals(other.NullableSbyte, NullableSbyte) &&
                Equals(other.NullableUshort, NullableUshort) &&
                Equals(other.NullableUint, NullableUint) &&
                Equals(other.NullableUlong, NullableUlong) &&
                Equals(other.NullableText, NullableText) &&
                Equals(other.NullableTimestamp, NullableTimestamp) &&
                DefaultComparer.Equals(other.NullableString, NullableString) &&
                Equals(other.NullableUri, NullableUri);
        }
    }
}
