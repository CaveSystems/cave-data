using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using Cave.IO;

namespace Cave.Data;

/// <summary>Provides writing of csv files using a struct or class.</summary>
[SuppressMessage("Style", "IDE0060")]
public sealed class DatWriter : IDisposable
{
    #region Private Fields

    readonly RowLayout layout;
    DataWriter? writer;

    #endregion Private Fields

    #region Private Methods

    static void WriteFieldDefinition(DataWriter writer, RowLayout layout, int version)
    {
        if (version < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(version));
        }

        if (version > 5)
        {
            throw new NotSupportedException($"Version {version} not supported!");
        }

        try
        {
            writer.Write("DatTable");
            writer.Write7BitEncoded32(version);
            writer.WritePrefixed(layout.Name);
            writer.Write7BitEncoded32(layout.FieldCount);
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var field = layout[i];
                writer.WritePrefixed(field.Name);
                writer.Write7BitEncoded32((int)field.DataType);
                writer.Write7BitEncoded32((int)field.Flags);
                switch (field.DataType)
                {
                    case DataType.User:
                    case DataType.String:
                        if (version > 2)
                        {
                            writer.Write7BitEncoded32((int)field.StringEncoding);
                        }

                        break;

                    case DataType.DateTime:
                        if (version > 1)
                        {
                            writer.Write7BitEncoded32((int)field.DateTimeKind);
                            writer.Write7BitEncoded32((int)field.DateTimeType);
                        }

                        break;

                    case DataType.TimeSpan:
                        if (version > 3)
                        {
                            writer.Write7BitEncoded32((int)field.DateTimeType);
                        }

                        break;
                }

                if ((field.DataType & DataType.MaskRequireValueType) != 0)
                {
                    var typeName = field.ValueType?.AssemblyQualifiedName ?? throw new InvalidOperationException($"Field[{i}].ValueType {field.ValueType} cannot be resolved!");
                    var parts = typeName.Split(',');
                    typeName = $"{parts[0]},{parts[1]}";
                    writer.WritePrefixed(typeName);
                }
            }

            writer.Flush();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Could not write field definition!", ex);
        }
    }

    byte[] GetData(Row row, int version)
    {
        if (version < 1) { throw new ArgumentOutOfRangeException(nameof(version)); }
        if (version > 5) { throw new NotSupportedException("Version not supported!"); }

        using var buffer = new MemoryStream();
        var w = new DataWriter(buffer);
        for (var i = 0; i < layout.FieldCount; i++)
        {
            var value = row[i];
            var fieldProperties = layout[i];
            var nullable = fieldProperties.IsNullable;
            switch (fieldProperties.DataType)
            {
                case DataType.Binary: WriteBinary(version, w, value, nullable); break;
                case DataType.Bool: WriteBool(version, w, value, nullable); break;
                case DataType.TimeSpan: WriteTimeSpan(version, w, value, nullable); break;
                case DataType.DateTime: WriteDateTime(version, w, value, nullable); break;
                case DataType.Single: Write(version, w, value as float?, nullable); break;
                case DataType.Double: Write(version, w, value as double?, nullable); break;
                case DataType.Decimal: Write(version, w, value as decimal?, nullable); break;
                case DataType.Int8: Write(version, w, value as sbyte?, nullable); break;
                case DataType.UInt8: Write(version, w, value as byte?, nullable); break;
                case DataType.Int16: Write(version, w, value as short?, nullable); break;
                case DataType.UInt16: Write(version, w, value as ushort?, nullable); break;
                case DataType.Int32: Write(version, w, value as int?, nullable); break;
                case DataType.UInt32: Write(version, w, value as uint?, nullable); break;
                case DataType.Int64: Write(version, w, value as long?, nullable); break;
                case DataType.UInt64: Write(version, w, value as ulong?, nullable); break;
                case DataType.Char: Write(version, w, value as char?, nullable); break;
                case DataType.Enum: WriteEnum(version, w, value, nullable); break;
                case DataType.Guid: WriteGuid(version, w, value as Guid?, nullable); break;
                case DataType.String:
                case DataType.User: WriteString(version, w, value, nullable); break;
                default: throw new NotImplementedException($"Datatype {fieldProperties.DataType} not implemented!");
            }
        }

        return buffer.ToArray();
    }

    void Write(int version, DataWriter writer, sbyte? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 1;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, byte? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 1;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, short? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case 2:
            case 3 or 4: writer.Write7BitEncoded32(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 2;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, char? value, bool allowNull)
    {
        switch (version)
        {
            case 1:
            case < 5: writer.Write(value ?? (char)0); break;
            case >= 5:
            {
                if (!allowNull) goto case 1;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, ushort? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case 2:
            case 3 or 4: writer.Write7BitEncoded32(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 2;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, int? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case 2:
            case 3 or 4: writer.Write7BitEncoded32(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 2;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, uint? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case 2:
            case 3 or 4: writer.Write7BitEncoded32(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 2;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, long? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case 2:
            case 3 or 4: writer.Write7BitEncoded64(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 2;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter writer, ulong? value, bool allowNull)
    {
        switch (version)
        {
            case 1: writer.Write(value ?? 0); break;
            case 2:
            case 3 or 4: writer.Write7BitEncoded64(value ?? 0); break;
            case >= 5:
            {
                if (!allowNull) goto case 2;
                writer.WritePrefixed(value);
                break;
            }
            default: throw new NotImplementedException();
        }
    }

    void Write(int version, DataWriter w, float? value, bool allowNull)
    {
        if (allowNull && version >= 5)
        {
            w.WritePrefixed(value);
            return;
        }
        w.Write(value ?? 0f);
    }

    void Write(int version, DataWriter w, double? value, bool allowNull)
    {
        if (allowNull && version >= 5)
        {
            w.WritePrefixed(value);
            return;
        }
        w.Write(value ?? 0d);
    }

    void Write(int version, DataWriter w, decimal? value, bool allowNull)
    {
        if (allowNull && version >= 5)
        {
            w.WritePrefixed(value);
            return;
        }
        w.Write(value ?? 0m);
    }

    void WriteBinary(int version, DataWriter writer, object? value, bool allowNull)
    {
        switch (value)
        {
            case byte[] block:
            {
                if (version < 3)
                {
                    writer.Write(block.Length);
                    writer.Write(block);
                }
                else
                {
                    writer.WritePrefixed(block);
                }
                break;
            }
            case null:
            {
                switch (version)
                {
                    case 1:
                    case 2: writer.Write(0); break;
                    case 3:
                    case 4: writer.Write7BitEncoded32(0); break;
                    case 5:
                    {
                        if (!allowNull) goto case 3;
                        writer.Write7BitEncoded32(-1);
                        break;
                    }
                    default: throw new NotImplementedException();
                }
                break;
            }
            default: throw new InvalidOperationException($"Cannot serialize {value} to byte[].");
        }
    }

    void WriteBool(int version, DataWriter w, object? value, bool allowNull)
    {
        switch (value)
        {
            case bool b: w.Write(b); break;
            case null:
            {
                if (!allowNull)
                {
                    w.Write(false);
                }
                else
                {
                    w.Write((byte)(version < 5 ? 0 : 0xff));
                }
                break;
            }
            default: throw new InvalidOperationException($"Cannot serialize {value} to bool.");
        }
    }

    void WriteData(byte[] data)
    {
        if (writer is null) throw new ObjectDisposedException(nameof(DatWriter));
        var entrySize = data.Length + BitCoder32.GetByteCount7BitEncoded(data.Length + 10);
        var start = writer.BaseStream.Position;
        BitCoder32.Write7BitEncoded(writer, entrySize);
        writer.Write(data);
        var fill = start + entrySize - writer.BaseStream.Position;
        if (fill > 0)
        {
            writer.Write(new byte[fill]);
        }
        else if (fill < 0)
        {
            throw new IOException("Container too small!");
        }
    }

    void WriteDateTime(int version, DataWriter w, object? value, bool allowNull)
    {
        var ticks = (value as DateTime?)?.Ticks;
        if (allowNull && version >= 5)
        {
            w.WritePrefixed(ticks);
            return;
        }
        w.Write(ticks ?? 0L);
    }

    void WriteEnum(int version, DataWriter w, object? value, bool allowNull)
    {
        var enumValue = Convert.ToInt64(value ?? 0, CultureInfo.CurrentCulture);
        if (allowNull && version >= 5)
        {
            if (value is null)
            {
                w.Write((byte)0);
            }
            else
            {
                w.WritePrefixed(enumValue);
            }
            return;
        }

        w.Write7BitEncoded64(enumValue);
    }

    void WriteGuid(int version, DataWriter w, Guid? value, bool allowNull)
    {
        if (!allowNull) value ??= Guid.Empty;
        WriteString(version, w, value?.ToString("D"), allowNull);
    }

    void WriteString(int version, DataWriter w, object? value, bool allowNull)
    {
        if (!allowNull) value ??= string.Empty;
        w.WritePrefixed(value?.ToString());
    }

    void WriteTimeSpan(int version, DataWriter w, object? value, bool allowNull)
    {
        var ticks = (value as TimeSpan?)?.Ticks;
        if (allowNull && version >= 5)
        {
            w.WritePrefixed(ticks);
            return;
        }
        w.Write(ticks ?? 0L);
    }

    #endregion Private Methods

    #region Public Fields

    /// <summary>The current version.</summary>
    public const int CurrentVersion = 5;

    #endregion Public Fields

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="DatWriter"/> class.</summary>
    /// <param name="layout">Table layout.</param>
    /// <param name="fileName">Filename to write to.</param>
    public DatWriter(RowLayout layout, string fileName)
        : this(layout, File.Create(fileName))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DatWriter"/> class.</summary>
    /// <param name="layout">Table layout.</param>
    /// <param name="stream">Stream to write to.</param>
    public DatWriter(RowLayout layout, Stream stream)
    {
        if (stream == null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        writer = new DataWriter(stream);
        this.layout = layout;
        WriteFieldDefinition(writer, this.layout, CurrentVersion);
    }

    #endregion Public Constructors

    #region Public Methods

    /// <summary>Creates a new dat file with the specified name and writes the whole table.</summary>
    /// <param name="fileName">Filename to write to.</param>
    /// <param name="table">Table to write.</param>
    public static void WriteTable(string fileName, ITable table)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        using var writer = new DatWriter(table.Layout, fileName);
        writer.WriteTable(table);
    }

    /// <summary>Closes the writer and the stream.</summary>
    public void Close()
    {
        if (writer != null)
        {
            writer.Close();
            writer = null;
        }
    }

    /// <inheritdoc/>
    public void Dispose() => Close();

    /// <summary>Writes a row to the file.</summary>
    /// <param name="row">Row to write.</param>
    public void Write(Row row)
    {
        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        var data = GetData(row, CurrentVersion);
        WriteData(data);
    }

    /// <summary>Writes a row to the file.</summary>
    /// <param name="value">Row to write.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    public void Write<TStruct>(TStruct value)
        where TStruct : struct =>
        Write(new Row(layout, layout.GetValues(value), false));

    /// <summary>Writes a number of rows to the file.</summary>
    /// <param name="table">Table to write.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    public void WriteRows<TStruct>(IEnumerable<TStruct> table)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        foreach (var dataSet in table)
        {
            var row = new Row(layout, layout.GetValues(dataSet), false);
            var data = GetData(row, CurrentVersion);
            WriteData(data);
        }
    }

    /// <summary>Writes a number of rows to the file.</summary>
    /// <param name="table">Table to write.</param>
    public void WriteTable(IEnumerable<Row> table)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        foreach (var row in table)
        {
            var data = GetData(row, CurrentVersion);
            WriteData(data);
        }
    }

    /// <summary>Writes a full table of rows to the file.</summary>
    /// <param name="table">Table to write.</param>
    public void WriteTable(ITable table)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        foreach (var row in table.GetRows())
        {
            var data = GetData(row, CurrentVersion);
            WriteData(data);
        }
    }

    #endregion Public Methods
}
