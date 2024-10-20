using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using Cave.IO;
using Cave.Security;

namespace Cave.Data;

/// <summary>Provides reading of dat files to a struct / class.</summary>
[SuppressMessage("Style", "IDE0060")]
public sealed class DatReader : IDisposable
{
    #region Private Fields

    DataReader? reader;

    #endregion Private Fields

    #region Private Methods

    static byte[]? ReadBinary(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case <= 2:
            {
                var size = reader.ReadInt32();
                return reader.ReadBytes(size);
            }
            case >= 3:
            {
                var size = reader.Read7BitEncodedInt32();
                if (size < 0) return !allowNull ? null : throw new NullReferenceException("Got null value at a not nullable field!");
                if (size == 0) return [];
                return reader.ReadBytes(size);
            }
            default: throw new NotImplementedException();
        }
    }

    static Guid? ReadGuid(int version, DataReader reader, bool allowNull)
    {
        var str = ReadString(version, reader, allowNull);
        return str is null ? null : Guid.Parse(str);
    }

    static bool? ReadBool(int version, DataReader reader, bool allowNull)
    {
        var b = reader.ReadByte();
        switch (b)
        {
            case 0: return false;
            case 1: return true;
            case 0xFF:
            {
                return !allowNull ? null : throw new NullReferenceException("Got null value at a not nullable field!");
            }
            default: throw new InvalidDataException("Invalid data at bool value!");
        }
    }

    static char? ReadChar(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1:
            case < 5:
            {
                return reader.ReadChar();
            }
            case >= 5:
            {
                if (!allowNull) goto case 1;
                return (char?)reader.ReadPrefixedInt32();
            }
            default: throw new NotImplementedException();
        }
    }

    static DateTime? ReadDateTime(int version, DataReader reader, DateTimeKind fieldKind, bool allowNull)
    {
        if (!allowNull || version < 5)
        {
            return new DateTime(reader.ReadInt64(), fieldKind);
        }
        var ticks = reader.ReadPrefixedInt64();
        return ticks is null ? null : new DateTime(ticks.Value, fieldKind);
    }

    static decimal? ReadDecimal(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1:
            case < 5:
            {
                return reader.ReadDecimal();
            }
            case >= 5:
            {
                if (!allowNull) goto case 1;
                return reader.ReadPrefixedDecimal();
            }
            default: throw new NotImplementedException();
        }
    }

    static double? ReadDouble(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1:
            case < 5:
            {
                return reader.ReadDouble();
            }
            case >= 5:
            {
                if (!allowNull) goto case 1;
                return reader.ReadPrefixedDouble();
            }
            default: throw new NotImplementedException();
        }
    }

    static object? ReadEnum(int version, DataReader reader, bool allowNull, Type enumType)
    {
        if (allowNull && version >= 5)
        {
            var enumValue = reader.ReadPrefixedInt64();
            if (enumValue is null) return null;
            return Convert.ChangeType(enumValue, enumType);
        }
        else
        {
            var enumValue = reader.Read7BitEncodedInt64();
            return Convert.ChangeType(enumValue, enumType);
        }
    }

    static float? ReadFloat(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1:
            case < 5:
            {
                return reader.ReadSingle();
            }
            case >= 5:
            {
                if (!allowNull) goto case 1;
                return reader.ReadPrefixedSingle();
            }
            default: throw new NotImplementedException();
        }
    }

    static short? ReadInt16(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadInt16();
            case 2:
            case 3 or 4:
            {
                return (short)reader.Read7BitEncodedInt32();
            }
            case >= 5:
            {
                if (!allowNull) goto case 2;
                return reader.ReadPrefixedInt16();
            }
            default: throw new NotImplementedException();
        }
    }

    static int? ReadInt32(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadInt32();
            case 2:
            case 3 or 4:
            {
                return reader.Read7BitEncodedInt32();
            }
            case >= 5:
            {
                if (!allowNull) goto case 2;
                return reader.ReadPrefixedInt32();
            }
            default: throw new NotImplementedException();
        }
    }

    static long? ReadInt64(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadInt64();
            case 2:
            case 3 or 4:
            {
                return reader.Read7BitEncodedInt64();
            }
            case >= 5:
            {
                if (!allowNull) goto case 2;
                return reader.ReadPrefixedInt64();
            }
            default: throw new NotImplementedException();
        }
    }

    static sbyte? ReadInt8(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadInt8();
            case >= 5:
            {
                if (!allowNull) goto case 1;
                return reader.ReadPrefixedInt8();
            }
            default: throw new NotImplementedException();
        }
    }

    static string? ReadString(int version, DataReader reader, bool allowNull) => reader.ReadPrefixedString() ?? (allowNull ? null : string.Empty);

    static TimeSpan? ReadTimeSpan(int version, DataReader reader, bool allowNull)
    {
        if (!allowNull || version < 5)
        {
            return new TimeSpan(reader.ReadInt64());
        }
        var ticks = reader.ReadPrefixedInt64();
        return ticks is null ? null : new TimeSpan(ticks.Value);
    }

    static ushort? ReadUInt16(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadUInt16();
            case 2:
            case 3 or 4:
            {
                return (ushort)reader.Read7BitEncodedUInt32();
            }
            case >= 5:
            {
                if (!allowNull) goto case 2;
                return reader.ReadPrefixedUInt16();
            }
            default: throw new NotImplementedException();
        }
    }

    static uint? ReadUInt32(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadUInt32();
            case 2:
            case 3 or 4:
            {
                return reader.Read7BitEncodedUInt32();
            }
            case >= 5:
            {
                if (!allowNull) goto case 2;
                return reader.ReadPrefixedUInt32();
            }
            default: throw new NotImplementedException();
        }
    }

    static ulong? ReadUInt64(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadUInt64();
            case 2:
            case 3 or 4:
            {
                return reader.Read7BitEncodedUInt64();
            }
            case >= 5:
            {
                if (!allowNull) goto case 2;
                return reader.ReadPrefixedUInt64();
            }
            default: throw new NotImplementedException();
        }
    }

    static byte? ReadUInt8(int version, DataReader reader, bool allowNull)
    {
        switch (version)
        {
            case 1: return reader.ReadUInt8();
            case >= 5:
            {
                if (!allowNull) goto case 1;
                return reader.ReadPrefixedUInt8();
            }
            default: throw new NotImplementedException();
        }
    }

    void Load(Stream stream)
    {
        if (stream == null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        reader = new DataReader(stream);
        Layout = LoadFieldDefinition(reader, out var version);
        Version = version;
    }

    #endregion Private Methods

    #region Internal Methods

    internal static RowLayout LoadFieldDefinition(DataReader reader, out int version)
    {
        var dateTimeKind = DateTimeKind.Unspecified;
        var dateTimeType = DateTimeType.Undefined;
        var stringEncoding = StringEncoding.UTF_8;
        if (reader.ReadString(8) != "DatTable")
        {
            throw new FormatException();
        }

        version = reader.Read7BitEncodedInt32();
        if (version is < 1 or > 5)
        {
            throw new InvalidDataException("Unknown Table version!");
        }

        // read name and create layout
        var layoutName = reader.ReadPrefixedString() ?? $"Table{Base32.Safe.Encode(RNG.UInt32)}";
        var fieldCount = reader.Read7BitEncodedInt32();
        var fields = new FieldProperties[fieldCount];
        for (var i = 0; i < fieldCount; i++)
        {
            var fieldName = reader.ReadPrefixedString() ?? throw new InvalidDataException("FieldName is unset!");
            var dataType = (DataType)reader.Read7BitEncodedInt32();
            var fieldFlags = (FieldFlags)reader.Read7BitEncodedInt32();
            var databaseDataType = dataType;
            switch (dataType)
            {
                case DataType.Enum:
                    databaseDataType = DataType.Int64;
                    break;
                case DataType.Guid:
                    databaseDataType = DataType.Guid;
                    break;
                case DataType.User:
                case DataType.String:
                    databaseDataType = DataType.String;
                    if (version > 2)
                    {
                        stringEncoding = (StringEncoding)reader.Read7BitEncodedInt32();
                    }
                    else
                    {
                        stringEncoding = StringEncoding.UTF_8;
                    }

                    break;

                case DataType.TimeSpan:
                    if (version > 3)
                    {
                        dateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
                    }

                    break;

                case DataType.DateTime:
                    if (version > 1)
                    {
                        dateTimeKind = (DateTimeKind)reader.Read7BitEncodedInt32();
                        dateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
                    }
                    else
                    {
                        dateTimeKind = DateTimeKind.Utc;
                        dateTimeType = DateTimeType.BigIntHumanReadable;
                    }

                    break;
            }

            Type? valueType = null;
            if ((dataType & DataType.MaskRequireValueType) != 0)
            {
                var typeName = reader.ReadPrefixedString();
                valueType = AppDom.FindType(typeName.BeforeFirst(','), typeName.AfterFirst(',').Trim());
            }

            var field = fields[i] = new FieldProperties
            {
                Index = i,
                Flags = fieldFlags,
                DataType = dataType,
                ValueType = valueType,
                Name = fieldName,
                TypeAtDatabase = databaseDataType,
                NameAtDatabase = fieldName,
                DateTimeType = dateTimeType,
                DateTimeKind = dateTimeKind,
                StringEncoding = stringEncoding
            };
            field.Validate();
        }

        return RowLayout.CreateUntyped(layoutName, fields);
    }

    internal static Row? ReadCurrentRow(DataReader reader, int version, RowLayout layout)
    {
        if (version < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(version));
        }

        if (version > 5)
        {
            throw new NotSupportedException("Version not supported!");
        }

        var dataStart = reader.BaseStream.Position;
        var dataSize = reader.Read7BitEncodedInt32();
        if (dataSize == 0)
        {
            return null;
        }

        var values = new object?[layout.FieldCount];
        for (var i = 0; i < layout.FieldCount; i++)
        {
            var field = layout[i];
            var dataType = field.DataType;
            var nullable = field.IsNullable;
            switch (dataType)
            {
                case DataType.Binary: values[i] = ReadBinary(version, reader, nullable); break;
                case DataType.Bool: values[i] = ReadBool(version, reader, nullable); break;
                case DataType.DateTime: values[i] = ReadDateTime(version, reader, field.DateTimeKind, nullable); break;
                case DataType.TimeSpan: values[i] = ReadTimeSpan(version, reader, nullable); break;
                case DataType.Int8: values[i] = ReadInt8(version, reader, nullable); break;
                case DataType.UInt8: values[i] = ReadUInt8(version, reader, nullable); break;
                case DataType.Int16: values[i] = ReadInt16(version, reader, nullable); break;
                case DataType.UInt16: values[i] = ReadUInt16(version, reader, nullable); break;
                case DataType.Int32: values[i] = ReadInt32(version, reader, nullable); break;
                case DataType.UInt32: values[i] = ReadUInt32(version, reader, nullable); break;
                case DataType.Int64: values[i] = ReadInt64(version, reader, nullable); break;
                case DataType.UInt64: values[i] = ReadUInt64(version, reader, nullable); break;
                case DataType.Char: values[i] = ReadChar(version, reader, nullable); break;
                case DataType.Single: values[i] = ReadFloat(version, reader, nullable); break;
                case DataType.Double: values[i] = ReadDouble(version, reader, nullable); break;
                case DataType.Decimal: values[i] = ReadDecimal(version, reader, nullable); break;
                case DataType.String: values[i] = ReadString(version, reader, nullable); break;
                case DataType.Enum: values[i] = ReadEnum(version, reader, nullable, field.ValueType ?? throw new InvalidOperationException("Parsing enum requires the Field.ValueType to be set!")); break;
                case DataType.Guid: values[i] = ReadGuid(version, reader, nullable); break;
                case DataType.User:
                {
                    var user = ReadString(version, reader, nullable);
                    values[i] = user is null ? null : layout.ParseValue(i, user, null);
                    break;
                }
                default: throw new NotImplementedException($"Datatype {dataType} not implemented!");
            }
        }

        var skip = dataStart + dataSize - reader.BaseStream.Position;
        if (skip < 0)
        {
            throw new FormatException();
        }

        if (skip > 0)
        {
            reader.BaseStream.Seek(skip, SeekOrigin.Current);
        }

        return new Row(layout, values, false);
    }

    #endregion Internal Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="DatReader"/> class.</summary>
    /// <param name="fileName">Filename to read from.</param>
    public DatReader(string fileName)
    {
        Stream stream = File.OpenRead(fileName);
        try
        {
            Load(stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>Initializes a new instance of the <see cref="DatReader"/> class.</summary>
    /// <param name="stream">Stream to read from.</param>
    public DatReader(Stream stream) => Load(stream);

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the layout of the table.</summary>
    public RowLayout Layout { get; private set; } = RowLayout.None;

    /// <summary>Gets the version the database was created with.</summary>
    public int Version { get; private set; }

    #endregion Public Properties

    #region Public Methods

    /// <summary>Reads a whole table from the specified dat file.</summary>
    /// <param name="table">Table to read the dat file into.</param>
    /// <param name="fileName">File name of the dat file.</param>
    public static void ReadTable(ITable table, string fileName)
    {
        using var reader = new DatReader(fileName);
        reader.ReadTable(table);
    }

    /// <summary>Reads a whole table from the specified dat file.</summary>
    /// <param name="table">Table to read the dat file into.</param>
    /// <param name="stream">The stream.</param>
    public static void ReadTable(ITable table, Stream stream)
    {
        using var reader = new DatReader(stream);
        reader.ReadTable(table);
    }

    /// <summary>Closes the reader.</summary>
    public void Close()
    {
        if (reader != null)
        {
            reader.Close();
            reader = null;
        }
    }

    /// <summary>Disposes the base stream.</summary>
    public void Dispose() => Close();

    /// <summary>Reads the whole file to a new list.</summary>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    /// <returns>A new <see cref="List{TStruct}"/>.</returns>
    public IList<TStruct> ReadList<TStruct>()
        where TStruct : struct
    {
        if (reader is null) throw new ObjectDisposedException(nameof(DatReader));
        var layout = RowLayout.CreateTyped(typeof(TStruct));
        RowLayout.CheckLayout(Layout, layout);
        if (!Layout.IsTyped)
        {
            Layout = layout;
        }

        var result = new List<TStruct>();
        while (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            var row = ReadCurrentRow(reader, Version, layout);
            if (row != null)
            {
                result.Add(row.GetStruct<TStruct>(layout));
            }
        }

        return result;
    }

    /// <summary>Reads a row from the file.</summary>
    /// <param name="checkLayout">Check layout prior read.</param>
    /// <param name="row">The read row.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    /// <returns>Returns true is the row was read, false otherwise.</returns>
    public bool ReadRow<TStruct>(bool checkLayout, out TStruct row)
        where TStruct : struct
    {
        if (reader is null) throw new ObjectDisposedException(nameof(DatReader));
        var layout = RowLayout.CreateTyped(typeof(TStruct));
        if (checkLayout)
        {
            RowLayout.CheckLayout(Layout, layout);
        }

        if (!Layout.IsTyped)
        {
            Layout = layout;
        }

        while (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            var currentRow = ReadCurrentRow(reader, Version, layout);
            if (currentRow != null)
            {
                row = currentRow.GetStruct<TStruct>(layout);
                return true;
            }
        }

        row = default;
        return false;
    }

    /// <summary>
    /// Reads the whole file to the specified table. This does not write transactions and does not clear the table. If you want to start with a clean table
    /// clear it prior using this function.
    /// </summary>
    /// <param name="table">Table to read to.</param>
    public void ReadTable(ITable table)
    {
        if (reader is null) throw new ObjectDisposedException(nameof(DatReader));
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        RowLayout.CheckLayout(Layout, table.Layout);
        if (!Layout.IsTyped)
        {
            Layout = table.Layout;
        }

        while (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            var row = ReadCurrentRow(reader, Version, Layout);
            if (row != null)
            {
                table.Insert(row);
            }
        }
    }

    #endregion Public Methods
}
