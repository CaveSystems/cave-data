#region CopyRight 2018
/*
    Copyright (c) 2005-2018 Andreas Rohleder (andreas@rohleder.cc)
    All rights reserved
*/
#endregion
#region License LGPL-3
/*
    This program/library/sourcecode is free software; you can redistribute it
    and/or modify it under the terms of the GNU Lesser General Public License
    version 3 as published by the Free Software Foundation subsequent called
    the License.

    You may not use this program/library/sourcecode except in compliance
    with the License. The License is included in the LICENSE file
    found at the installation directory or the distribution package.

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#endregion License
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion Authors & Contributors

using Cave.IO;
using System;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides Row based serialization
    /// </summary>
    public static class RowSerializer
    {
        /// <summary>
        /// Settings used during de/serialization
        /// </summary>
        public enum Flags
        {
            /// <summary>No flags</summary>
            None = 0,

            /// <summary>Serialize the layout first, then the data. This adds type safety to the stream but costs a lot of bandwith and time.</summary>
            WithLayout = 1,
        }

        #region private Data Serializer
        static void SerializeData(DataWriter writer, RowLayout layout, Row row)
        {
            for (int i = 0; i < layout.FieldCount; i++)
            {
                DataType dataType = layout.GetProperties(i).DataType;
                switch (dataType)
                {
                    case DataType.Binary:
                        {
                            byte[] data = (byte[])row.GetValue(i);
                            if (data == null)
                        {
                            data = new byte[0];
                        }

                        writer.Write7BitEncoded32(data.Length);
                            writer.Write(data);
                            break;
                        }
                    case DataType.Bool: writer.Write((bool)row.GetValue(i)); break;
                    case DataType.TimeSpan: writer.Write(((TimeSpan)row.GetValue(i))); break;
                    case DataType.DateTime: writer.Write(((DateTime)row.GetValue(i))); break;
                    case DataType.Single: writer.Write((float)row.GetValue(i)); break;
                    case DataType.Double: writer.Write((double)row.GetValue(i)); break;
                    case DataType.Int8: writer.Write((sbyte)row.GetValue(i)); break;
                    case DataType.Int16: writer.Write((short)row.GetValue(i)); break;
                    case DataType.UInt8: writer.Write((byte)row.GetValue(i)); break;
                    case DataType.UInt16: writer.Write((ushort)row.GetValue(i)); break;
                    case DataType.Int32: writer.Write7BitEncoded32((int)row.GetValue(i)); break;
                    case DataType.Int64: writer.Write7BitEncoded64((long)row.GetValue(i)); break;
                    case DataType.UInt32: writer.Write7BitEncoded32((uint)row.GetValue(i)); break;
                    case DataType.UInt64: writer.Write7BitEncoded64((ulong)row.GetValue(i)); break;
                    case DataType.Char: writer.Write((char)row.GetValue(i)); break;
                    case DataType.Decimal: writer.Write((decimal)row.GetValue(i)); break;

                    case DataType.String:
                    case DataType.User:
                        {
                            object data = row.GetValue(i);
                            string str = (data == null) ? null : data.ToString();
                            writer.WritePrefixed(str);
                            break;
                        }

                    case DataType.Enum:
                        {
                            long value = Convert.ToInt64(row.GetValue(i));
                            writer.Write7BitEncoded64(value);
                            break;
                        }

                    default:
                        throw new NotImplementedException(string.Format("Datatype {0} not implemented!", dataType));
                }
            }
        }
        #endregion

        #region Serializer
        /// <summary>Serializes the specified items without layout.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writer">The writer.</param>
        /// <param name="items">The items.</param>
        public static void Serialize<T>(this DataWriter writer, params T[] items) where T : struct
        {
            Serialize(writer, 0, items);
        }

        /// <summary>Serializes the specified items.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writer">The writer.</param>
        /// <param name="flags">The settings used during serialization.</param>
        /// <param name="items">The items.</param>
        /// <exception cref="ArgumentNullException">
        /// Items
        /// or
        /// Writer
        /// </exception>
        public static void Serialize<T>(this DataWriter writer, Flags flags, params T[] items) where T : struct
        {
            if (items == null)
            {
                throw new ArgumentNullException("Items");
            }

            if (writer == null)
            {
                throw new ArgumentNullException("Writer");
            }

            writer.Write7BitEncoded32((int)flags);
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            if (0 != (flags & Flags.WithLayout))
            {
                layout.Save(writer);
            }
            writer.Write7BitEncoded64(items.Length);
            foreach (T item in items)
            {
                SerializeData(writer, layout, Row.Create(layout, item));
            }
        }

        /// <summary>Serializes the specified table.</summary>
        /// <param name="writer">The writer.</param>
        /// <param name="flags">The settings used during serialization.</param>
        /// <param name="table">The table.</param>
        /// <exception cref="ArgumentNullException">
        /// Table
        /// or
        /// Writer
        /// </exception>
        public static void Serialize(this DataWriter writer, Flags flags, ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            if (writer == null)
            {
                throw new ArgumentNullException("Writer");
            }

            writer.Write7BitEncoded32((int)flags);
            if (0 != (flags & Flags.WithLayout))
            {
                table.Layout.Save(writer);
            }
            writer.Write7BitEncoded64(table.RowCount);
            foreach(Row row in table.GetRows())
            {
                SerializeData(writer, table.Layout, row);
            }
        }

        /// <summary>Serializes the specified table.</summary>
        /// <param name="writer">The writer.</param>
        /// <param name="flags">The flags.</param>
        /// <param name="layout">The layout.</param>
        /// <param name="rows">The rows.</param>
        /// <exception cref="ArgumentNullException">
        /// Rows
        /// or
        /// Layout
        /// or
        /// Writer
        /// </exception>
        public static void Serialize(this DataWriter writer, Flags flags, RowLayout layout, params Row[] rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (writer == null)
            {
                throw new ArgumentNullException("Writer");
            }

            writer.Write7BitEncoded32((int)flags);
            if (0 != (flags & Flags.WithLayout))
            {
                layout.Save(writer);
            }
            writer.Write7BitEncoded64(rows.Length);
            foreach (Row row in rows)
            {
                SerializeData(writer, layout, row);
            }
        }
        #endregion

        #region private Data Deserializer
        static Row DeserializeData(DataReader reader, RowLayout layout)
        {
            object[] row = new object[layout.FieldCount];
            for (int i = 0; i < layout.FieldCount; i++)
            {
                DataType dataType = layout.GetProperties(i).DataType;

                switch (dataType)
                {
                    case DataType.Binary:
                        int size = reader.Read7BitEncodedInt32();
                        row[i] = reader.ReadBytes(size);
                        break;

                    case DataType.Bool: row[i] = reader.ReadBool(); break;
                    case DataType.DateTime: row[i] = reader.ReadDateTime(); break;
                    case DataType.TimeSpan: row[i] = reader.ReadTimeSpan(); break;
                    case DataType.Int8: row[i] = reader.ReadInt8(); break;
                    case DataType.Int16: row[i] = reader.ReadInt16(); break;
                    case DataType.Int32: row[i] = reader.Read7BitEncodedInt32(); break;
                    case DataType.Int64: row[i] = reader.Read7BitEncodedInt64(); break;
                    case DataType.UInt32: row[i] = reader.Read7BitEncodedUInt32(); break;
                    case DataType.UInt64: row[i] = reader.Read7BitEncodedUInt64(); break;
                    case DataType.UInt8: row[i] = reader.ReadUInt8(); break;
                    case DataType.UInt16: row[i] = reader.ReadUInt16(); break;
                    case DataType.Char: row[i] = reader.ReadChar(); break;
                    case DataType.Single: row[i] = reader.ReadSingle(); break;
                    case DataType.Double: row[i] = reader.ReadDouble(); break;
                    case DataType.Decimal: row[i] = reader.ReadDecimal(); break;
                    case DataType.String: row[i] = reader.ReadString(); break;
                    case DataType.Enum: row[i] = reader.Read7BitEncodedInt64(); break;
                    case DataType.User: row[i] = reader.ReadString(); break;

                    default: throw new NotImplementedException(string.Format("Datatype {0} not implemented!", dataType));
                }
            }
            return new Row(row);
        }
        #endregion

        #region Deserializer
        /// <summary>Deserializes an item array.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Reader</exception>
        public static T[] DeserializeItems<T>(this DataReader reader) where T : struct
        {
            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }

            Flags flags = (Flags)reader.Read7BitEncodedInt32();
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            if (0 != (flags & Flags.WithLayout))
            {
                RowLayout otherLayout = RowLayout.Load(reader);
                RowLayout.CheckLayout(layout, otherLayout);
            }
            T[] results = new T[reader.Read7BitEncodedInt32()];
            for (int i = 0; i < results.Length; i++)
            {
                Row row = DeserializeData(reader, layout);
                results[i] = row.GetStruct<T>(layout);
            }
            return results;
        }

        /// <summary>Deserializes a table.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static ITable<T> DeserializeTable<T>(this DataReader reader) where T : struct
        {
            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }

            Flags flags = (Flags)reader.Read7BitEncodedInt32();
            MemoryTable<T> result = new MemoryTable<T>();
            if (0 != (flags & Flags.WithLayout))
            {
                RowLayout layout = RowLayout.Load(reader);
                result.Storage.CheckLayout(result.Layout, layout);
            }
            long count = reader.Read7BitEncodedInt64();
            for (long l = 0; l < count; l++)
            {
                Row row = DeserializeData(reader, result.Layout);
                result.Insert(row);
            }
            return result;
        }

        /// <summary>Deserializes a structure.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static T DeserializeStruct<T>(this DataReader reader) where T : struct
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            Row row = DeserializeRow(reader, layout);
            return row.GetStruct<T>(layout);
        }

        /// <summary>Deserializes a row.</summary>
        /// <param name="reader">The reader.</param>
        /// <param name="layout">The layout.</param>
        /// <returns></returns>
        /// <exception cref="InvalidDataException"></exception>
        public static Row DeserializeRow(this DataReader reader, RowLayout layout)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }

            Flags flags = (Flags)reader.Read7BitEncodedInt32();
            if (0 != (flags & Flags.WithLayout))
            {
                RowLayout otherLayout = RowLayout.Load(reader);
                RowLayout.CheckLayout(layout, otherLayout);
            }
            int count = reader.Read7BitEncodedInt32();
            if (count != 1)
            {
                throw new InvalidDataException(string.Format("Got {0} Rows at the stream but want to read exactly one!", count));
            }

            return DeserializeData(reader, layout);
        }
        #endregion

        #region Foreign Deserializer (needs layout at stream)
        /// <summary>Deserializes a foreign row.</summary>
        /// <remarks>This can only deserialize rows written with layout. (Requires use of <see cref="Flags.WithLayout"/> when serializing.)</remarks>
        /// <param name="reader">The reader to read from.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Reader</exception>
        /// <exception cref="NotSupportedException">For DeserializeForeignX() functions the layout has to be written by the sender! The current decoded data does not contain a layout!</exception>
        /// <exception cref="InvalidDataException"></exception>
        public static Row DeserializeForeignRow(this DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }

            Flags flags = (Flags)reader.Read7BitEncodedInt32();
            if (0 == (flags & Flags.WithLayout))
            {
                throw new NotSupportedException("For DeserializeForeignX() functions the layout has to be written by the sender! The current decoded data does not contain a layout!");
            }
            RowLayout layout = RowLayout.Load(reader);
            int count = reader.Read7BitEncodedInt32();
            if (count != 1)
            {
                throw new InvalidDataException(string.Format("Got {0} Rows at the stream but want to read exactly one!", count));
            }

            return DeserializeData(reader, layout);
        }

        /// <summary>Deserializes a foreign table.</summary>
        /// <remarks>This can only deserialize rows written with layout. (Requires use of <see cref="Flags.WithLayout"/> when serializing.)</remarks>
        /// <param name="reader">The reader to read from.</param>
        /// <returns></returns>
        public static ITable DeserializeForeignTable(this DataReader reader) 
        {
            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }

            Flags flags = (Flags)reader.Read7BitEncodedInt32();
            if (0 == (flags & Flags.WithLayout))
            {
                throw new NotSupportedException("For DeserializeForeignX() functions the layout has to be written by the sender! The current decoded data does not contain a layout!");
            }
            RowLayout layout = RowLayout.Load(reader);
            MemoryTable result = new MemoryTable(layout);
            long count = reader.Read7BitEncodedInt64();
            for (long l = 0; l < count; l++)
            {
                Row row = DeserializeData(reader, layout);
                result.Insert(row);
            }
            return result;
        }
        #endregion
    }
}