using System.ComponentModel;
using Cave;
using Cave.Data;
using Cave.Security;

namespace dbtest;

#pragma warning disable CS1591

public struct TestRow
{
    public static TestRow Create()
    {
        return new TestRow()
        {
            Blob = RNG.GetBytes(RNG.UInt16 % 30),
            Byte = RNG.UInt8,
            Short = RNG.Int16,
            Int = RNG.Int32,
            Long = RNG.Int64,
            Duration = RNG.TimeSpanAny,
            Enabled = (RNG.Int64 % 2) == 0,
            Guid = Guid.NewGuid(),
            SByte = RNG.Int8,
            UShort = RNG.UInt16,
            UInt = RNG.UInt32,
            ULong = RNG.UInt64,
            Text = RNG.GetUnicode(20),
            String = RNG.GetAscii(20),
            DateTime = DateTime.UtcNow,
            Uri = new Uri($"http://server{RNG.UInt16}"),
            NullableBlob = (RNG.Bool) ? null : RNG.GetBytes(RNG.UInt16 % 30),
            NullableByte = (RNG.Bool) ? null : RNG.UInt8,
            NullableDuration = (RNG.Bool) ? null : RNG.TimeSpanMilliSeconds,
            NullableEnabled = (RNG.Bool) ? null : RNG.Bool,
            NullableGuid = (RNG.Bool) ? null : Guid.NewGuid(),
            NullableInt = (RNG.Bool) ? null : RNG.Int32,
            NullableLong = (RNG.Bool) ? null : RNG.UInt32,
            NullableSByte = (RNG.Bool) ? null : RNG.Int8,
            NullableShort = (RNG.Bool) ? null : RNG.Int16,
            NullableString = (RNG.Bool) ? null : RNG.GetAscii(20),
            NullableText = (RNG.Bool) ? null : RNG.GetUnicode(20),
            NullableDateTime = (RNG.Bool) ? null : RNG.DateTime,
            NullableUInt = (RNG.Bool) ? null : RNG.UInt32,
            NullableULong = (RNG.Bool) ? null : RNG.UInt64,
            NullableUri = (RNG.Bool) ? null : new Uri($"http://server{RNG.UInt32}"),
            NullableUShort = (RNG.Bool) ? null : RNG.UInt16,
        };
    }

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
    [TimeSpanFormat(DateTimeType.BigIntTicks)]
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
    public DateTime DateTime;

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
    [TimeSpanFormat(DateTimeType.BigIntMilliSeconds)]
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
    public DateTime? NullableDateTime;

    [Field]
    public string? NullableString;

    [Field]
    public Uri? NullableUri;

    #endregion Public Fields
}
