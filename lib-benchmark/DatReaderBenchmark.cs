using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using Cave.Data;
using Microsoft.VSDiagnostics;

namespace Cave.Data.Benchmarks;

[Table("BenchStruct")]
public struct BenchStruct
{
    public static BenchStruct Create(int i) => new()
    {
        ID = i,
        B = (byte)(i & 0xFF),
        SB = (sbyte)(-i / 10),
        US = (ushort)i,
        C = (char)i,
        I = i,
        F = (500 - i) * 0.5f,
        D = (500 - i) * 0.5d,
        Date = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Local).AddSeconds(i),
        Time = TimeSpan.FromSeconds(i),
        S = (short)(i - 500),
        UI = (uint)i,
        Text = i.ToString(),
        Dec = 0.005m * (i - 500)
    };
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
    public float F;
    [Field]
    public double D;
    [Field]
    public DateTime Date;
    [Field]
    public TimeSpan Time;
    [Field(Length = 32)]
    public string Text;
    [Field]
    public decimal Dec;
}

[CPUUsageDiagnoser]
public class DatReaderBenchmark
{
    byte[] data = Array.Empty<byte>();
    const int RowCount = 10_000;
    [GlobalSetup]
    public void Setup()
    {
        var stream = new MemoryStream();
        var writer = new DatWriter(RowLayout.CreateTyped(typeof(BenchStruct)), stream);
        for (var i = 0; i < RowCount; i++)
        {
            writer.Write(BenchStruct.Create(i));
        }

        data = stream.ToArray();
    }

    [Benchmark]
    public int ReadRowLoop()
    {
        using var stream = new MemoryStream(data);
        using var reader = new DatReader(stream);
        var count = 0;
        while (reader.ReadRow<BenchStruct>(false, out _))
        {
            count++;
        }

        return count;
    }

    [Benchmark]
    public int ReadListAll()
    {
        using var stream = new MemoryStream(data);
        using var reader = new DatReader(stream);
        return reader.ReadList<BenchStruct>().Count;
    }
}
