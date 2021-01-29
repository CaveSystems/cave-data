using System;
using System.ComponentModel;
using System.Linq;
using Cave;
using Cave.Collections;
using Cave.Data;
using Cave.IO;

namespace Test.Cave
{
    public enum TestEnum
    {
        A,
        B,
        C
    }

    [Table("SmallTestStruct")]
    struct SmallTestStruct
    {
        [Field(Flags = FieldFlags.AutoIncrement | FieldFlags.ID)]
        public long ID;

        [Field]
        public string Content;

        [Field]
        public DateTime DateTime;

        [Field]
        public string Name;

        [Field]
        public string Source;

        [Field]
        public TestEnum Level;

        [Field]
        public int Integer;

        public override string ToString() => $"{ID} {Content} {DateTime} {Name} {Source} {Level} {Integer}";
    }

    [Table("TestStructBug")]
    struct TestStructBug
    {
        static readonly Environment.SpecialFolder[] EnumValues = Enum.GetValues(typeof(Environment.SpecialFolder)).Cast<Environment.SpecialFolder>().ToArray();

        public static TestStructBug Create(int i)
        {
            var t = new TestStructBug
            {
                IDField = (ulong) (i * i),
                AutoIncUniqueIndexedField = i * i * i,
                AutoIncIndexField = (byte) (i & 0xFF),
                BuggyField = i.ToString(),
                IndexedField = (uint) i,
                NoField = i.ToString(),
                SomeEnum = EnumValues[i % EnumValues.Length],
                UniqueIndexedField = (sbyte) (-i / 10),
                UniqueField = (short) i,
                AutoIncField = (ushort) i
            };
            return t;
        }

        [Field(Flags = FieldFlags.ID, Name = "Field1", Length = 1)]
        [Description("Fabulous Field A")]
        public ulong IDField;

        [Field(Flags = FieldFlags.Index, Name = "Field2", Length = 2)]
        [Description("Fabulous Field B")]
        public uint IndexedField;

        [Field(Flags = FieldFlags.Unique, Name = "Field3", Length = 3)]
        [Description("Fabulous Field C")]
        public short UniqueField;

        [Field(Flags = FieldFlags.AutoIncrement, Name = "Field4", Length = 4)]
        [Description("Fabulous Field D")]
        public ushort AutoIncField;

        [Field(Flags = FieldFlags.Index | FieldFlags.AutoIncrement, Name = "Field5", Length = 5)]
        [Description("Fabulous Field E")]
        public byte AutoIncIndexField;

        [Field(Flags = FieldFlags.Unique | FieldFlags.Index, Name = "Field6", Length = 6)]
        [Description("Fabulous Field F")]
        public sbyte UniqueIndexedField;

        [Field(Flags = FieldFlags.Index | FieldFlags.AutoIncrement | FieldFlags.Unique, Name = "Field7", Length = 7)]
        [Description("Fabulous Field G")]
        public long AutoIncUniqueIndexedField;

        [Field(Name = "Field8", Length = 8)]
        [Description("Fabulous Field H")]
        public Environment.SpecialFolder SomeEnum;

        public string NoField;

        [Field(Name = null)]
        [Description(null)]
        public string BuggyField;
    }

    [Table("TestStructClean")]
    public struct TestStructClean
    {
        public static TestStructClean Create(int i)
        {
            var t = new TestStructClean
            {
                Arr = BitConverterLE.Instance.GetBytes((long) i),
                B = (byte) (i & 0xFF),
                SB = (sbyte) (-i / 10),
                US = (ushort) i,
                C = (char) i,
                I = i,
                F = (500 - i) * 0.5f,
                D = (500 - i) * 0.5d,
                Date = new DateTime(1 + Math.Abs(i), 12, 31, 23, 59, 48, i % 1000, DateTimeKind.Local),
                Time = TimeSpan.FromSeconds(i),
                S = (short) (i - 500),
                UI = (uint) i,
                Text = i.ToString(),
                Dec = 0.005m * (i - 500),
                Uri = new Uri("http://localhost/" + i),
                ConStr = "http://localhost/" + i
            };
            return t;
        }

        [Field(Flags = FieldFlags.AutoIncrement | FieldFlags.ID)]
        public long ID;

        [Field]
        public byte B;

        [Field]
        public sbyte SB;

        [Field]
        public char C;

        [Field]
        public short S;

        [Field]
        public ushort US;

        [Field]
        public int I;

        [Field]
        public uint UI;

        [Field]
        public byte[] Arr;

        [Field]
        public string Text;

        [Field]
        public TimeSpan Time;

        [Field]
        public DateTime Date;

        [Field]
        public double D;

        [Field]
        public float F;

        [Field]
        public decimal Dec;

        [Field]
        public Uri Uri;

        [Field]
        public ConnectionString ConStr;

        public override bool Equals(object obj)
        {
            if (!(obj is TestStructClean))
            {
                return false;
            }

            var other = (TestStructClean) obj;
            return
                DefaultComparer.Equals(Arr, other.Arr) &&
                Equals(B, other.B) &&
                Equals(C, other.C) &&
                Equals(ConStr, other.ConStr) &&
                Equals(D, other.D) &&
                Equals(Date, other.Date) &&
                Equals(Dec, other.Dec) &&
                Equals(F, other.F) &&
                Equals(I, other.I) &&
                Equals(S, other.S) &&
                Equals(SB, other.SB) &&
                Equals(Text, other.Text) &&
                Equals(Time, other.Time) &&
                Equals(UI, other.UI) &&
                Equals(Uri, other.Uri) &&
                Equals(US, other.US);
        }

        public override int GetHashCode() => ID.GetHashCode();
    }
}
