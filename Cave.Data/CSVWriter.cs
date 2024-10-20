using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Cave.IO;

namespace Cave.Data;

/// <summary>Provides writing of csv files using a struct or class.</summary>
public sealed class CsvWriter : IDisposable
{
    #region Private Fields

    DataWriter? writer;

    #endregion Private Fields

    #region Private Methods

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    void Dispose(bool disposing)
    {
        if (disposing)
        {
            writer?.BaseStream.Dispose();
            writer = null;
        }
    }

    #endregion Private Methods

    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="CsvWriter"/> class.</summary>
    /// <param name="layout">The table layout.</param>
    /// <param name="fileName">Filename to write to.</param>
    /// <param name="properties">Extended properties.</param>
    public CsvWriter(RowLayout layout, string fileName, CsvProperties properties = default)
        : this(layout, File.Create(fileName), properties, true)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CsvWriter"/> class.</summary>
    /// <param name="properties">Extended properties.</param>
    /// <param name="layout">The table layout.</param>
    /// <param name="stream">The stream.</param>
    /// <param name="closeBaseStream">if set to <c>true</c> [close base stream on close].</param>
    public CsvWriter(RowLayout layout, Stream stream, CsvProperties properties = default, bool closeBaseStream = false)
    {
        BaseStream = stream ?? throw new ArgumentNullException(nameof(stream));
        Layout = layout ?? throw new ArgumentNullException(nameof(layout));
        Properties = properties.Valid ? properties : CsvProperties.Default;
        CloseBaseStream = closeBaseStream;
        writer = new DataWriter(stream, Properties.Encoding, Properties.NewLineMode);
        if (Properties.NoHeader)
        {
            return;
        }

        // write header
        for (var i = 0; i < Layout.FieldCount; i++)
        {
            if (i > 0)
            {
                writer.Write(Properties.Separator);
            }

            if (Properties.StringMarker.HasValue)
            {
                writer.Write(Properties.StringMarker.Value);
            }

            writer.Write(Layout[i].NameAtDatabase);
            if (Properties.StringMarker.HasValue)
            {
                writer.Write(Properties.StringMarker.Value);
            }
        }

        writer.WriteLine();
        writer.Flush();
    }

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the underlying base stream.</summary>
    public Stream BaseStream { get; }

    /// <summary>Gets a value indicating whether [close base stream on close].</summary>
    /// <value><c>true</c> if [close base stream on close]; otherwise, <c>false</c>.</value>
    public bool CloseBaseStream { get; }

    /// <summary>Gets the row layout.</summary>
    public RowLayout Layout { get; }

    /// <summary>Gets the <see cref="CsvProperties"/>.</summary>
    public CsvProperties Properties { get; }

    #endregion Public Properties

    #region Public Methods

    /// <summary>Creates a string representation of the specified row.</summary>
    /// <typeparam name="TStruct">The structure type.</typeparam>
    /// <param name="row">The row.</param>
    /// <param name="provider">The format provider used for each field.</param>
    /// <returns>Returns a new string representing the row.</returns>
    public static string RowToString<TStruct>(TStruct row, IFormatProvider provider)
        where TStruct : struct
    {
        var layout = RowLayout.CreateTyped(typeof(TStruct));
        return RowToString(CsvProperties.Default, layout, layout.GetRow(row), provider);
    }

    /// <summary>Creates a string representation of the specified row.</summary>
    /// <param name="properties">The csv properties.</param>
    /// <param name="layout">The row layout.</param>
    /// <param name="row">The row.</param>
    /// <param name="provider">The format provider used for each field (optional, defaults to properties.Format).</param>
    /// <returns>Returns a new string representing the row.</returns>
    public static string RowToString(CsvProperties properties, RowLayout layout, Row row, IFormatProvider? provider = null)
    {
        if (layout == null)
        {
            throw new ArgumentNullException(nameof(layout));
        }

        if (row == null)
        {
            throw new ArgumentNullException(nameof(row));
        }

        provider ??= properties.Format;

        var result = new StringBuilder();
        var values = row.Values;
        for (var i = 0; i < layout.FieldCount; i++)
        {
            if (i > 0)
            {
                result.Append(properties.Separator);
            }

            var value = values[layout.Fields[i].Index];
            if (value is not null)
            {
                var field = layout[i];
                switch (field.DataType)
                {
                    case DataType.Binary:
                    {
                        var str = Base64.NoPadding.Encode((byte[])value);
                        result.Append(str);
                        break;
                    }
                    case DataType.Bool:
                    case DataType.Int8:
                    case DataType.Int16:
                    case DataType.Int32:
                    case DataType.Int64:
                    case DataType.UInt8:
                    case DataType.UInt16:
                    case DataType.UInt32:
                    case DataType.UInt64:
                    {
                        if (!properties.SaveDefaultValues && value.Equals(0))
                        {
                            break;
                        }

                        var str = value.ToString();
                        result.Append(str);
                        break;
                    }
                    case DataType.Char:
                    {
                        if (!properties.SaveDefaultValues && value.Equals((char)0))
                        {
                            break;
                        }

                        var str = value.ToString();
                        result.Append(str);
                        break;
                    }
                    case DataType.Decimal:
                    {
                        if (!properties.SaveDefaultValues && value.Equals(0m))
                        {
                            break;
                        }

                        var dec = (decimal)value;
                        result.Append(dec.ToString(provider));
                        break;
                    }
                    case DataType.Single:
                    {
                        if (!properties.SaveDefaultValues && value.Equals(0f))
                        {
                            break;
                        }

                        var f = (float)value;
                        result.Append(f.ToString("R", provider));
                        break;
                    }
                    case DataType.Double:
                    {
                        if (!properties.SaveDefaultValues && value.Equals(0d))
                        {
                            break;
                        }

                        var d = (double)value;
                        result.Append(d.ToString("R", provider));
                        break;
                    }
                    case DataType.TimeSpan:
                    {
                        if (!properties.SaveDefaultValues && value.Equals(TimeSpan.Zero))
                        {
                            break;
                        }

                        var str = field.GetString(value, $"{properties.StringMarker}", provider);
                        result.Append(str);
                        break;
                    }
                    case DataType.DateTime:
                    {
                        if (!properties.SaveDefaultValues && value.Equals(new DateTime(0)))
                        {
                            break;
                        }

                        string str;
                        if (properties.DateTimeFormat != null)
                        {
                            str = ((DateTime)value).ToString(properties.DateTimeFormat, provider);
                        }
                        else
                        {
                            str = field.GetString(value, $"{properties.StringMarker}", provider);
                        }

                        result.Append(str);
                        break;
                    }
                    case DataType.User:
                    case DataType.String:
                    {
                        if (!properties.SaveDefaultValues && string.IsNullOrEmpty(value as string))
                        {
                            break;
                        }

                        var str = value == null ? string.Empty : value.ToString()!;
                        str = str.EscapeUtf8();
                        if (properties.StringMarker.HasValue)
                        {
                            str = str.Replace($"{properties.StringMarker}", $"{properties.StringMarker}{properties.StringMarker}");
                            result.Append(properties.StringMarker);
                        }

                        if (str.Length == 0)
                        {
                            result.Append(' ');
                        }
                        else
                        {
                            if (properties.StringMarker.HasValue)
                            {
                                if (str.StartsWith(properties.StringMarker.Value.ToString(), StringComparison.Ordinal))
                                {
                                    result.Append(' ');
                                }
                            }

                            result.Append(str);
                            if (properties.StringMarker.HasValue)
                            {
                                if (str.EndsWith(properties.StringMarker.Value.ToString(), StringComparison.Ordinal))
                                {
                                    result.Append(' ');
                                }
                            }
                        }

                        if (properties.StringMarker.HasValue)
                        {
                            result.Append(properties.StringMarker);
                        }

                        break;
                    }
                    case DataType.Enum:
                    {
                        if (!properties.SaveDefaultValues && Convert.ToInt32(value, provider).Equals(0))
                        {
                            break;
                        }

                        var str = $"{value}";
                        if (properties.StringMarker.HasValue)
                        {
                            str = str.Box(properties.StringMarker.Value);
                        }
                        result.Append(str);
                        break;
                    }
                    case DataType.Guid:
                    {
                        var guid = new Guid($"{value}");
                        if (guid == Guid.Empty)
                        {
                            //guid is invalid or empty
                            if (!properties.SaveDefaultValues)
                            {
                                break;
                            }
                        }
                        var str = guid.ToString("D");
                        if (properties.StringMarker.HasValue)
                        {
                            str = str.Box(properties.StringMarker.Value);
                        }
                        result.Append(str);
                        break;
                    }
                    default:
                        throw new NotImplementedException($"DataType {layout[i].DataType} is not implemented!");
                }
            }
        }

        return result.ToString();
    }

    /// <summary>Creates a new csv file with the specified name and writes the whole table.</summary>
    /// <param name="rows">Table to write to the csv file.</param>
    /// <param name="fileName">File name of the csv file.</param>
    /// <param name="properties">Properties of the csv file.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    public static void WriteAlien<TStruct>(IEnumerable<TStruct> rows, string fileName, CsvProperties properties = default)
        where TStruct : struct
    {
        var layout = RowLayout.CreateAlien(typeof(TStruct), false);
        var writer = new CsvWriter(layout, fileName, properties);
        try
        {
            writer.Write(rows);
        }
        finally
        {
            writer.Close();
        }
    }

    /// <summary>Creates a new csv file with the specified name and writes the whole table.</summary>
    /// <param name="rows">Rows to write to the csv file.</param>
    /// <param name="fileName">File name of the csv file.</param>
    /// <param name="properties">Properties of the csv file.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    public static void WriteRows<TStruct>(IEnumerable<TStruct> rows, string fileName, CsvProperties properties = default)
        where TStruct : struct
    {
        var layout = RowLayout.CreateTyped(typeof(TStruct));
        var writer = new CsvWriter(layout, fileName, properties);
        try
        {
            writer.Write(rows);
        }
        finally
        {
            writer.Close();
        }
    }

    /// <summary>Creates a new csv file with the specified name and writes the whole table.</summary>
    /// <param name="table">Table to write to the csv file.</param>
    /// <param name="fileName">File name of the csv file.</param>
    /// <param name="properties">Properties of the csv file.</param>
    public static void WriteTable(ITable table, string fileName, CsvProperties properties = default)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var writer = new CsvWriter(table.Layout, fileName, properties);
        try
        {
            writer.Write(table);
        }
        finally
        {
            writer.Close();
        }
    }

    /// <summary>Creates a new csv file with the specified name and writes the whole table.</summary>
    /// <param name="table">Table to write to the csv file.</param>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="properties">Properties of the csv file.</param>
    public static void WriteTable(ITable table, Stream stream, CsvProperties properties = default)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var writer = new CsvWriter(table.Layout, stream, properties);
        try
        {
            writer.Write(table);
        }
        finally
        {
            writer.Close();
        }
    }

    /// <summary>Closes the writer and the stream.</summary>
    public void Close()
    {
        if (CloseBaseStream)
        {
            writer?.Close();
        }

        writer = null;
    }

    /// <inheritdoc/>
    public void Dispose() => Dispose(true);

    /// <summary>Writes a row to the file.</summary>
    /// <param name="row">The row to write.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    public void Write<TStruct>(TStruct row)
        where TStruct : struct =>
        WriteRow(new Row(Layout, Layout.GetValues(row), false));

    /// <summary>Writes a number of rows to the file.</summary>
    /// <param name="table">Table to write.</param>
    /// <typeparam name="TStruct">Structure type.</typeparam>
    public void Write<TStruct>(IEnumerable<TStruct> table)
        where TStruct : struct
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        foreach (var row in table)
        {
            Write(row);
        }
    }

    /// <summary>Writes a full table of rows to the file.</summary>
    /// <param name="table">Table to write.</param>
    public void Write(ITable table)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        foreach (var row in table.GetRows())
        {
            WriteRow(row);
        }
    }

    /// <summary>Writes a row to the file.</summary>
    /// <param name="row">Row to write.</param>
    public void WriteRow(Row row) => writer?.WriteLine(RowToString(Properties, Layout, row));

    /// <summary>Writes a number of rows to the file.</summary>
    /// <param name="table">Table to write.</param>
    public void WriteRows(IEnumerable<Row> table)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        foreach (var row in table)
        {
            WriteRow(row);
        }
    }

    #endregion Public Methods
}
