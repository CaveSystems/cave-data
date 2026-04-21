using System.IO;
using Cave.Data;
using NUnit.Framework;

namespace Test.Cave.Data;

[TestFixture]
public class DatReaderWriter
{
    #region Public Methods

    [Test]
    public void StructReadWrite()
    {
        var stream = new MemoryStream();
        var writer = new DatWriter(RowLayout.CreateTyped(typeof(TestStructClean)), stream);
        for (var i = 0; i < 100; i++)
        {
            var t = TestStructClean.Create(i);
            writer.Write(t);
        }

        stream.Seek(0, SeekOrigin.Begin);
        var reader = new DatReader(stream);
        for (var i = 0; i < 100; i++)
        {
            Assert.IsTrue(reader.ReadRow<TestStructClean>(true, out var t));
            Assert.AreEqual(t, TestStructClean.Create(i));
        }
    }

    [Table("nonNullableInt32Row")]
    public struct NonNullableInt32Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Name = "value", Length = 11)]
        public int Value;

        #endregion Public Fields
    }

    [Table("nullableBinaryRow")]
    public struct NullableBinaryRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "payload", Length = 16777215)]
        public byte[]? Payload;

        #endregion Public Fields
    }

    [Table("nullableBoolRow")]
    public struct NullableBoolRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 1)]
        public bool? Value;

        #endregion Public Fields
    }

    [Table("nullableCharRow")]
    public struct NullableCharRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 1)]
        public char? Value;

        #endregion Public Fields
    }

    [Table("nullableDateTimeRow")]
    public struct NullableDateTimeRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 30)]
        public System.DateTime? Value;

        #endregion Public Fields
    }

    [Table("nullableDecimalRow")]
    public struct NullableDecimalRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 29)]
        public decimal? Value;

        #endregion Public Fields
    }

    [Table("nullableDoubleRow")]
    public struct NullableDoubleRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 22)]
        public double? Value;

        #endregion Public Fields
    }

    [Table("nullableEnumRow")]
    public struct NullableEnumRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 11)]
        public NullableRoundtripEnum? Value;

        #endregion Public Fields
    }

    [Table("nullableInt16Row")]
    public struct NullableInt16Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 11)]
        public short? Value;

        #endregion Public Fields
    }

    [Table("nullableInt32Row")]
    public struct NullableInt32Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 11)]
        public int? Value;

        #endregion Public Fields
    }

    [Table("nullableInt64Row")]
    public struct NullableInt64Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 20)]
        public long? Value;

        #endregion Public Fields
    }

    [Table("nullableInt8Row")]
    public struct NullableInt8Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 4)]
        public sbyte? Value;

        #endregion Public Fields
    }

    [Table("nullableSingleRow")]
    public struct NullableSingleRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 11)]
        public float? Value;

        #endregion Public Fields
    }

    [Table("nullableStringRow")]
    public struct NullableStringRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 1024)]
        public string? Value;

        #endregion Public Fields
    }

    [Table("nullableTimeSpanRow")]
    public struct NullableTimeSpanRow
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 30)]
        public System.TimeSpan? Value;

        #endregion Public Fields
    }

    [Table("nullableUInt16Row")]
    public struct NullableUInt16Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 11)]
        public ushort? Value;

        #endregion Public Fields
    }

    [Table("nullableUInt32Row")]
    public struct NullableUInt32Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 11)]
        public uint? Value;

        #endregion Public Fields
    }

    [Table("nullableUInt64Row")]
    public struct NullableUInt64Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 20)]
        public ulong? Value;

        #endregion Public Fields
    }

    [Table("nullableUInt8Row")]
    public struct NullableUInt8Row
    {
        #region Public Fields

        [Field(Flags = FieldFlags.ID, Name = "id", Length = 11)]
        public int Id;

        [Field(Flags = FieldFlags.Nullable, Name = "value", Length = 4)]
        public byte? Value;

        #endregion Public Fields
    }

    #endregion Public Structs

    #region Public Enums

    public enum NullableRoundtripEnum
    {
        A,
        B,
        C
    }

    #endregion Public Enums

    #region Public Methods

    [Test]
    public void InMemoryRoundtripWithNullableInt32()
    {
        var storage = new MemoryStorage();
        var db = storage.CreateDatabase("minimalMemoryNullable");
        var table = db.CreateTable<NullableInt32Row>("nullableInt32Row");

        table.Insert(new NullableInt32Row { Id = 1, Value = 42 });
        table.Insert(new NullableInt32Row { Id = 2, Value = null });
        table.Insert(new NullableInt32Row { Id = 3, Value = 99 });

        var rows = table.GetStructs();
        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual(42, rows[0].Value);
        Assert.IsNull(rows[1].Value);
        Assert.AreEqual(99, rows[2].Value);
    }

    [Test]
    public void TestNullableBinaryRow()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteBinary");
            var writeTable = writeDb.CreateTable<NullableBinaryRow>("nullableBinaryRow");
            writeTable.Insert(new NullableBinaryRow { Id = 1, Payload = new byte[] { 1, 2, 3 } });
            writeTable.Insert(new NullableBinaryRow { Id = 2, Payload = null });
            writeTable.Insert(new NullableBinaryRow { Id = 3, Payload = new byte[0] });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadBinary");
            var readTable = readDb.CreateTable<NullableBinaryRow>("nullableBinaryRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);

            Assert.IsNotNull(rows[0].Payload);
            CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, rows[0].Payload);

            Assert.IsNull(rows[1].Payload);

            Assert.IsNotNull(rows[2].Payload);
            CollectionAssert.AreEqual(new byte[0], rows[2].Payload);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestNullableBoolRow()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteBool");
            var writeTable = writeDb.CreateTable<NullableBoolRow>("nullableBoolRow");
            writeTable.Insert(new NullableBoolRow { Id = 1, Value = true });
            writeTable.Insert(new NullableBoolRow { Id = 2, Value = null });
            writeTable.Insert(new NullableBoolRow { Id = 3, Value = false });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadBool");
            var readTable = readDb.CreateTable<NullableBoolRow>("nullableBoolRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(true, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(false, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestNullableStringRow()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteString");
            var writeTable = writeDb.CreateTable<NullableStringRow>("nullableStringRow");
            writeTable.Insert(new NullableStringRow { Id = 1, Value = "abc" });
            writeTable.Insert(new NullableStringRow { Id = 2, Value = null });
            writeTable.Insert(new NullableStringRow { Id = 3, Value = string.Empty });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadString");
            var readTable = readDb.CreateTable<NullableStringRow>("nullableStringRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual("abc", rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(string.Empty, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNonNullableInt32()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("minimalWriteNonNullable");
            var writeTable = writeDb.CreateTable<NonNullableInt32Row>("nonNullableInt32Row");
            writeTable.Insert(new NonNullableInt32Row { Id = 1, Value = 42 });
            writeTable.Insert(new NonNullableInt32Row { Id = 2, Value = 99 });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("minimalReadNonNullable");
            var readTable = readDb.CreateTable<NonNullableInt32Row>("nonNullableInt32Row");
            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(2, rows.Count);
            Assert.AreEqual(42, rows[0].Value);
            Assert.AreEqual(99, rows[1].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableChar()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableChar");
            var writeTable = writeDb.CreateTable<NullableCharRow>("nullableCharRow");
            writeTable.Insert(new NullableCharRow { Id = 1, Value = 'X' });
            writeTable.Insert(new NullableCharRow { Id = 2, Value = null });
            writeTable.Insert(new NullableCharRow { Id = 3, Value = '0' });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableChar");
            var readTable = readDb.CreateTable<NullableCharRow>("nullableCharRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual('X', rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual('0', rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableDateTime()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableDateTime");
            var writeTable = writeDb.CreateTable<NullableDateTimeRow>("nullableDateTimeRow");
            writeTable.Insert(new NullableDateTimeRow { Id = 1, Value = new System.DateTime(2024, 1, 2, 3, 4, 5, System.DateTimeKind.Utc) });
            writeTable.Insert(new NullableDateTimeRow { Id = 2, Value = null });
            writeTable.Insert(new NullableDateTimeRow { Id = 3, Value = new System.DateTime(2024, 1, 2, 4, 5, 6, System.DateTimeKind.Utc) });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableDateTime");
            var readTable = readDb.CreateTable<NullableDateTimeRow>("nullableDateTimeRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(new System.DateTime(2024, 1, 2, 3, 4, 5, System.DateTimeKind.Utc), rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(new System.DateTime(2024, 1, 2, 4, 5, 6, System.DateTimeKind.Utc), rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableDecimal()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableDecimal");
            var writeTable = writeDb.CreateTable<NullableDecimalRow>("nullableDecimalRow");
            writeTable.Insert(new NullableDecimalRow { Id = 1, Value = 42.5m });
            writeTable.Insert(new NullableDecimalRow { Id = 2, Value = null });
            writeTable.Insert(new NullableDecimalRow { Id = 3, Value = 0.25m });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableDecimal");
            var readTable = readDb.CreateTable<NullableDecimalRow>("nullableDecimalRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42.5m, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0.25m, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableDouble()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableDouble");
            var writeTable = writeDb.CreateTable<NullableDoubleRow>("nullableDoubleRow");
            writeTable.Insert(new NullableDoubleRow { Id = 1, Value = 42.5d });
            writeTable.Insert(new NullableDoubleRow { Id = 2, Value = null });
            writeTable.Insert(new NullableDoubleRow { Id = 3, Value = 0.25d });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableDouble");
            var readTable = readDb.CreateTable<NullableDoubleRow>("nullableDoubleRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42.5d, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0.25d, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableEnum()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableEnum");
            var writeTable = writeDb.CreateTable<NullableEnumRow>("nullableEnumRow");
            writeTable.Insert(new NullableEnumRow { Id = 1, Value = NullableRoundtripEnum.B });
            writeTable.Insert(new NullableEnumRow { Id = 2, Value = null });
            writeTable.Insert(new NullableEnumRow { Id = 3, Value = NullableRoundtripEnum.A });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableEnum");
            var readTable = readDb.CreateTable<NullableEnumRow>("nullableEnumRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(NullableRoundtripEnum.B, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(NullableRoundtripEnum.A, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableInt16()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableInt16");
            var writeTable = writeDb.CreateTable<NullableInt16Row>("nullableInt16Row");
            writeTable.Insert(new NullableInt16Row { Id = 1, Value = 42 });
            writeTable.Insert(new NullableInt16Row { Id = 2, Value = null });
            writeTable.Insert(new NullableInt16Row { Id = 3, Value = 0 });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableInt16");
            var readTable = readDb.CreateTable<NullableInt16Row>("nullableInt16Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual((short)42, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual((short)0, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableInt32()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullable");
            var writeTable = writeDb.CreateTable<NullableInt32Row>("nullableInt32Row");
            writeTable.Insert(new NullableInt32Row { Id = 1, Value = 42 });
            writeTable.Insert(new NullableInt32Row { Id = 2, Value = null });
            writeTable.Insert(new NullableInt32Row { Id = 3, Value = 0 });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullable");
            var readTable = readDb.CreateTable<NullableInt32Row>("nullableInt32Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableInt64()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableInt64");
            var writeTable = writeDb.CreateTable<NullableInt64Row>("nullableInt64Row");
            writeTable.Insert(new NullableInt64Row { Id = 1, Value = 42L });
            writeTable.Insert(new NullableInt64Row { Id = 2, Value = null });
            writeTable.Insert(new NullableInt64Row { Id = 3, Value = 0L });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableInt64");
            var readTable = readDb.CreateTable<NullableInt64Row>("nullableInt64Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42L, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0L, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableInt8()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableInt8");
            var writeTable = writeDb.CreateTable<NullableInt8Row>("nullableInt8Row");
            writeTable.Insert(new NullableInt8Row { Id = 1, Value = 42 });
            writeTable.Insert(new NullableInt8Row { Id = 2, Value = null });
            writeTable.Insert(new NullableInt8Row { Id = 3, Value = 0 });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableInt8");
            var readTable = readDb.CreateTable<NullableInt8Row>("nullableInt8Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual((sbyte)42, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual((sbyte)0, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableSingle()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableSingle");
            var writeTable = writeDb.CreateTable<NullableSingleRow>("nullableSingleRow");
            writeTable.Insert(new NullableSingleRow { Id = 1, Value = 42.5f });
            writeTable.Insert(new NullableSingleRow { Id = 2, Value = null });
            writeTable.Insert(new NullableSingleRow { Id = 3, Value = 0.25f });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableSingle");
            var readTable = readDb.CreateTable<NullableSingleRow>("nullableSingleRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42.5f, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0.25f, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableTimeSpan()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableTimeSpan");
            var writeTable = writeDb.CreateTable<NullableTimeSpanRow>("nullableTimeSpanRow");
            writeTable.Insert(new NullableTimeSpanRow { Id = 1, Value = System.TimeSpan.FromMinutes(42) });
            writeTable.Insert(new NullableTimeSpanRow { Id = 2, Value = null });
            writeTable.Insert(new NullableTimeSpanRow { Id = 3, Value = System.TimeSpan.Zero });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableTimeSpan");
            var readTable = readDb.CreateTable<NullableTimeSpanRow>("nullableTimeSpanRow");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(System.TimeSpan.FromMinutes(42), rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(System.TimeSpan.Zero, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableUInt16()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableUInt16");
            var writeTable = writeDb.CreateTable<NullableUInt16Row>("nullableUInt16Row");
            writeTable.Insert(new NullableUInt16Row { Id = 1, Value = 42 });
            writeTable.Insert(new NullableUInt16Row { Id = 2, Value = null });
            writeTable.Insert(new NullableUInt16Row { Id = 3, Value = 0 });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableUInt16");
            var readTable = readDb.CreateTable<NullableUInt16Row>("nullableUInt16Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual((ushort)42, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual((ushort)0, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableUInt32()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableUInt32");
            var writeTable = writeDb.CreateTable<NullableUInt32Row>("nullableUInt32Row");
            writeTable.Insert(new NullableUInt32Row { Id = 1, Value = 42u });
            writeTable.Insert(new NullableUInt32Row { Id = 2, Value = null });
            writeTable.Insert(new NullableUInt32Row { Id = 3, Value = 0u });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableUInt32");
            var readTable = readDb.CreateTable<NullableUInt32Row>("nullableUInt32Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42u, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0u, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableUInt64()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableUInt64");
            var writeTable = writeDb.CreateTable<NullableUInt64Row>("nullableUInt64Row");
            writeTable.Insert(new NullableUInt64Row { Id = 1, Value = 42UL });
            writeTable.Insert(new NullableUInt64Row { Id = 2, Value = null });
            writeTable.Insert(new NullableUInt64Row { Id = 3, Value = 0UL });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableUInt64");
            var readTable = readDb.CreateTable<NullableUInt64Row>("nullableUInt64Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual(42UL, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual(0UL, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }

    [Test]
    public void TestSaveReadRoundtripWithNullableUInt8()
    {
        var fileName = Path.GetTempFileName();
        try
        {
            var writeStorage = new MemoryStorage();
            var writeDb = writeStorage.CreateDatabase("mixedWriteNullableUInt8");
            var writeTable = writeDb.CreateTable<NullableUInt8Row>("nullableUInt8Row");
            writeTable.Insert(new NullableUInt8Row { Id = 1, Value = 42 });
            writeTable.Insert(new NullableUInt8Row { Id = 2, Value = null });
            writeTable.Insert(new NullableUInt8Row { Id = 3, Value = 0 });
            writeTable.SaveTo(fileName);

            var readStorage = new MemoryStorage();
            var readDb = readStorage.CreateDatabase("mixedReadNullableUInt8");
            var readTable = readDb.CreateTable<NullableUInt8Row>("nullableUInt8Row");

            DatReader.ReadTable(readTable, fileName);

            var rows = readTable.GetStructs();
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual((byte)42, rows[0].Value);
            Assert.IsNull(rows[1].Value);
            Assert.AreEqual((byte)0, rows[2].Value);
        }
        finally
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }
    }
    #endregion Public Methods
}
