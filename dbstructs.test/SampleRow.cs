using System.Diagnostics;
using Cave;
using Cave.Data;

namespace dbstructs.test;

#pragma warning disable CS1591

public struct SampleRow
{
    #region Public Fields

    [Field(Flags = FieldFlags.ID | FieldFlags.AutoIncrement, Name = "id")]
    public long Id;

    [Field]
    public byte[] Blob;

    [Field]
    public byte Byte;

    [Field]
    public TimeSpan Duration;

    [Field(Name = "enabled", Length = 4)]
    public bool Enabled;

    [Field]
    public Guid Guid;

    [Field]
    public sbyte SByte;

    [Field(Length = 30)]
    public UTF8 Text;

    [Field(Name = "timestamp")]
    public DateTime Timestamp;

    [Field(Name = "user_id")]
    public int UserId;

    #endregion Public Fields
}
