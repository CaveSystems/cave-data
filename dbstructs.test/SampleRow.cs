using System.ComponentModel;
using System.Diagnostics;
using Cave;
using Cave.Data;

namespace dbstructs.test;

#pragma warning disable CS1591

public struct SampleRow
{
    #region Public Fields

    [Field(Flags = FieldFlags.ID | FieldFlags.AutoIncrement, Name = "id")]
    [Description("Id field")]
    public long Id;

    [Field]
    public byte[] Blob;

    [Field]
    public byte Byte;

    [Field]
    public short Short;

    [Field]
    public int Int;

    [Field]
    public long Long;

    [Field]
    public TimeSpan Duration;

    [Field(Name = "enabled", Length = 4)]
    public bool Enabled;

    [Field]
    public Guid Guid;

    [Field]
    public sbyte SByte;

    [Field]
    public ushort UShort;

    [Field]
    public uint UInt;

    [Field]
    public ulong ULong;

    [Field(Length = 30)]
    public UTF8 Text;

    [Field]
    public string String;

    [Field(Name = "timestamp")]
    public DateTime Timestamp;

    [Field]
    public Uri Uri;

    [Field]
    public byte[]? NullableBlob;

    [Field]
    public byte? NullableByte;

    [Field]
    public short? NullableShort;

    [Field]
    public int? NullableInt;

    [Field]
    public long? NullableLong;

    [Field]
    public TimeSpan? NullableDuration;

    [Field]
    public bool? NullableEnabled;

    [Field]
    public Guid? NullableGuid;

    [Field]
    public sbyte? NullableSByte;

    [Field]
    public ushort? NullableUShort;

    [Field]
    public uint? NullableUInt;

    [Field]
    public ulong? NullableULong;

    [Field(Length = 30)]
    public UTF8? NullableText;

    [Field]
    public DateTime? NullableTimestamp;

    [Field]
    public string? NullableString;

    [Field]
    public Uri? NullableUri;

    #endregion Public Fields
}
