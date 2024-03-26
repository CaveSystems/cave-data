using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>Provides a row layout implementation.</summary>
    [DebuggerDisplay("{" + nameof(Name) + "} [{" + nameof(FieldCount) + "}]")]
    public sealed class RowLayout : IEquatable<RowLayout>, IEnumerable<IFieldProperties>
    {
        #region Private Fields

        static readonly Dictionary<string, RowLayout> layoutCache = new();

        readonly IList<IFieldProperties> fieldProperties;

        #endregion Private Fields

        #region Private Methods

        /// <summary>Saves the fieldproperties to the specified writer.</summary>
        /// <param name="writer">The writer.</param>
        /// <param name="field">Field properties to save.</param>
        static void Save(DataWriter writer, IFieldProperties field)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            writer.Write7BitEncoded32((int)field.DataType);
            writer.Write7BitEncoded32((int)field.TypeAtDatabase);
            writer.Write7BitEncoded32((int)field.Flags);
            writer.WritePrefixed(field.Name);
            writer.WritePrefixed(field.NameAtDatabase);
            var typeName =
                field.ValueType.AssemblyQualifiedName.Substring(0, field.ValueType.AssemblyQualifiedName.IndexOf(','));
            writer.WritePrefixed(typeName);
            if (field.DataType == DataType.DateTime)
            {
                writer.Write7BitEncoded32((int)field.DateTimeKind);
                writer.Write7BitEncoded32((int)field.DateTimeType);
            }

            if (field.DataType is DataType.String or DataType.User)
            {
                writer.Write(field.MaximumLength);
            }
        }

        static void SetValueInternal(ref object target, IFieldProperties field, object value, CultureInfo culture = null)
        {
            if ((value != null) && (value.GetType() != field.ValueType))
            {
                value = field.DataType switch
                {
                    DataType.User => field.ParseValue(value.ToString()),
                    DataType.Enum => Enum.Parse(field.ValueType, value.ToString(), true),
                    _ => Convert.ChangeType(value, field.ValueType, culture)
                };
            }

            field.FieldInfo.SetValue(target, value);
        }

        #endregion Private Methods

        #region Internal Constructors

        /// <summary>Initializes a new instance of the <see cref="RowLayout"/> class.</summary>
        /// <param name="name">The Name of the Layout.</param>
        /// <param name="fields">The fieldproperties to use.</param>
        /// <param name="rowtype">Dotnet row type.</param>
        internal RowLayout(string name, IFieldProperties[] fields, Type rowtype)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Invalid characters at table name!");
            }

            var indices = fields.Select(f => f.Index).ToList();
            if (indices.Distinct().Count() != fields.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(fields), "Fields do not use unique indices!");
            }

            MinIndex = indices.Min();
            MaxIndex = indices.Max();
            if (MinIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(fields), "Minimum index < 0!");
            }

            FieldCount = fields.Length;
            fieldProperties = fields;
            Name = name;
            RowType = rowtype;
        }

        #endregion Internal Constructors

        #region Public Fields

        /// <summary>Gets the field count.</summary>
        public readonly int FieldCount;

        /// <summary>Maximum field index.</summary>
        public readonly int MaxIndex;

        /// <summary>Minimum field index.</summary>
        public readonly int MinIndex;

        /// <summary>Gets the name of the layout.</summary>
        public readonly string Name;

        /// <summary>The row type.</summary>
        public readonly Type RowType;

        #endregion Public Fields

        #region Public Constructors

        /// <summary>Initializes a new instance of the <see cref="RowLayout"/> class.</summary>
        public RowLayout()
        {
            fieldProperties = new IFieldProperties[0];
            Name = "Undefined";
        }

        #endregion Public Constructors

        #region Public Properties

        /// <summary>Gets or sets a value indicating whether caching for known typed layouts is disabled or not.</summary>
        public static bool DisableLayoutCache { get; set; }

        /// <summary>Gets the field properties.</summary>
        /// <value>A new readonly collection instance containing all field properties.</value>
        public IList<IFieldProperties> Fields => new ReadOnlyCollection<IFieldProperties>(fieldProperties);

        /// <summary>Gets the fields marked with the <see cref="FieldFlags.ID"/>.</summary>
        public IEnumerable<IFieldProperties> Identifier => fieldProperties.Where(p => p.Flags.HasFlag(FieldFlags.ID));

        /// <summary>Gets a value indicating whether the layout was created from a typed struct or not.</summary>
        public bool IsTyped => RowType != null;

        #endregion Public Properties

        #region Public Indexers

        /// <summary>Gets the field properties of the field with the specified index.</summary>
        /// <param name="index">Field index.</param>
        /// <returns>The <see cref="Cave.FieldProperties"/> instance.</returns>
        public IFieldProperties this[int index] => fieldProperties[index];

        /// <summary>Gets the field properties of the field with the specified name.</summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>The <see cref="Cave.FieldProperties"/> instance.</returns>
        public IFieldProperties this[string fieldName] => fieldProperties[GetFieldIndex(fieldName, true)];

        #endregion Public Indexers

        #region Public Methods

        /// <summary>Checks two layouts for equality.</summary>
        /// <param name="expected">The expected layout.</param>
        /// <param name="current">The layout to check.</param>
        /// <param name="fieldPropertiesConversion">field conversion function to use.</param>
        public static void CheckLayout(RowLayout expected, RowLayout current, Func<IFieldProperties, IFieldProperties> fieldPropertiesConversion = null)
        {
            if (ReferenceEquals(expected, current))
            {
                return;
            }

            if (expected == null)
            {
                throw new ArgumentNullException(nameof(expected));
            }

            if (current == null)
            {
                throw new ArgumentNullException(nameof(current));
            }

            if (expected.FieldCount != current.FieldCount)
            {
                throw new InvalidDataException($"Fieldcount of table {current.Name} differs (found {current.FieldCount} expected {expected.FieldCount})!");
            }

            for (var i = 0; i < expected.FieldCount; i++)
            {
                var expectedField = expected[i];
                var currentField = current[i];
                if (fieldPropertiesConversion != null)
                {
                    expectedField = fieldPropertiesConversion(expectedField);
                    currentField = fieldPropertiesConversion(currentField);
                }

                if (!expectedField.Equals(currentField))
                {
                    throw new InvalidDataException($"Fieldproperties of table {current.Name} differ! (found {currentField} expected {expectedField})!");
                }
            }
        }

        /// <summary>Clears the layout cache.</summary>
        public static void ClearCache()
        {
            lock (layoutCache)
            {
                layoutCache.Clear();
            }
        }

        /// <summary>Creates an alien row layout without using any field properies.</summary>
        /// <param name="type">Type to parse fields from.</param>
        /// <param name="onlyPublic">if set to <c>true</c> [use only public].</param>
        /// <returns>A new <see cref="RowLayout"/> instance.</returns>
        public static RowLayout CreateAlien(Type type, bool onlyPublic) => CreateAlien(type, onlyPublic, NamingStrategy.Exact);

        /// <summary>Creates an alien row layout without using any field properies.</summary>
        /// <param name="type">Type to parse fields from.</param>
        /// <param name="onlyPublic">if set to <c>true</c> [use only public].</param>
        /// <param name="namingStrategy">Naming strategy for fields.</param>
        /// <returns>A new <see cref="RowLayout"/> instance.</returns>
        public static RowLayout CreateAlien(Type type, bool onlyPublic, NamingStrategy namingStrategy)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            var bindingFlags = BindingFlags.Public | BindingFlags.Instance;
            if (onlyPublic)
            {
                bindingFlags |= BindingFlags.NonPublic;
            }

            var rawInfos = type.GetFields(bindingFlags);
            var properties = new FieldProperties[rawInfos.Length];
            for (var i = 0; i < rawInfos.Length; i++)
            {
                var fieldInfo = rawInfos[i];
                try
                {
                    if (fieldInfo.FieldType.IsArray)
                    {
                        continue;
                    }

                    var field = new FieldProperties();
                    field.LoadFieldInfo(i, fieldInfo, namingStrategy);
                    properties[i] = field;
                }
                catch (Exception ex)
                {
                    throw new InvalidDataException($"Error while loading field properties of type {type.FullName} field {fieldInfo}!", ex);
                }
            }

            return new(type.Name, properties, type);
        }

        /// <summary>Creates a RowLayout instance for the specified struct.</summary>
        /// <param name="type">The type to build the rowlayout for.</param>
        /// <param name="excludedFields">The excluded fields.</param>
        /// <returns>A new <see cref="RowLayout"/> instance.</returns>
        public static RowLayout CreateTyped(Type type, string[] excludedFields) => CreateTyped(type, null, null, excludedFields);

        /// <summary>Creates a RowLayout instance for the specified struct.</summary>
        /// <param name="type">The type to build the rowlayout for.</param>
        /// <param name="nameOverride">The table name override.</param>
        /// <param name="storage">The Storage engine to use.</param>
        /// <param name="excludedFields">The excluded fields.</param>
        /// <returns>A new <see cref="RowLayout"/> instance.</returns>
        public static RowLayout CreateTyped(Type type, string nameOverride = null, IStorage storage = null, params string[] excludedFields)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            lock (layoutCache)
            {
                var cacheName = $"{type.FullName},{nameOverride}";
                if (!layoutCache.TryGetValue(cacheName, out var result))
                {
                    var isStruct = type.IsValueType && !type.IsEnum && !type.IsPrimitive;
                    if (!isStruct)
                    {
                        throw new ArgumentException($"Type {type} is not a struct! Only structs may be used as row definition!");
                    }

                    var tableAttribute = TableAttribute.Get(type);
                    var tableName = tableAttribute.NamingStrategy.GetNameByStrategy(tableAttribute.Name ?? type.Name);
                    if (!string.IsNullOrEmpty(nameOverride))
                    {
                        tableName = nameOverride;
                    }

                    if (tableName.HasInvalidChars(ASCII.Strings.SafeName))
                    {
                        throw new ArgumentException("Invalid characters at table name!");
                    }

                    var rawInfos = type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                    var properties = new List<IFieldProperties>(rawInfos.Length);
                    for (var i = 0; i < rawInfos.Length; i++)
                    {
                        var fieldInfo = rawInfos[i];
                        try
                        {
                            if (fieldInfo.GetCustomAttributes(typeof(FieldAttribute), false).Length == 0)
                            {
                                continue;
                            }

                            if (excludedFields != null)
                            {
                                if (Array.IndexOf(excludedFields, fieldInfo.Name) > -1)
                                {
                                    continue;
                                }
                            }

                            var fieldProperties = new FieldProperties();
                            fieldProperties.LoadFieldInfo(i, fieldInfo, tableAttribute.NamingStrategy);
                            properties.Add(storage == null ? fieldProperties : storage.GetDatabaseFieldProperties(fieldProperties));
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException($"Error while loading field properties of type {type.FullName} field {fieldInfo}!", ex);
                        }
                    }

                    result = new(tableName, properties.ToArray(), type);
                    if (!DisableLayoutCache)
                    {
                        layoutCache[cacheName] = result;
                    }
                }

                return result;
            }
        }

        /// <summary>Creates a new layout with the given name and field properties.</summary>
        /// <param name="name">Name of the layout.</param>
        /// <param name="fields">FieldProperties to use.</param>
        /// <returns>A new <see cref="RowLayout"/> instance.</returns>
        public static RowLayout CreateUntyped(string name, params IFieldProperties[] fields) => new(name, fields, null);

        /// <summary>Gets the <see cref="DataType"/> for a given <see cref="Type"/>.</summary>
        /// <param name="type">The <see cref="Type"/> to convert.</param>
        /// <returns>The data type.</returns>
        public static DataType DataTypeFromType(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            if (type == typeof(sbyte))
            {
                return DataType.Int8;
            }

            if (type == typeof(byte))
            {
                return DataType.UInt8;
            }

            if (type == typeof(short))
            {
                return DataType.Int16;
            }

            if (type == typeof(ushort))
            {
                return DataType.UInt16;
            }

            if (type == typeof(int))
            {
                return DataType.Int32;
            }

            if (type == typeof(uint))
            {
                return DataType.UInt32;
            }

            if (type == typeof(long))
            {
                return DataType.Int64;
            }

            if (type == typeof(ulong))
            {
                return DataType.UInt64;
            }

            if (type == typeof(char))
            {
                return DataType.Char;
            }

            if (type == typeof(string))
            {
                return DataType.String;
            }

            if (type == typeof(float))
            {
                return DataType.Single;
            }

            if (type == typeof(double))
            {
                return DataType.Double;
            }

            if (type == typeof(bool))
            {
                return DataType.Bool;
            }

            if (type == typeof(decimal))
            {
                return DataType.Decimal;
            }

            if (type == typeof(byte[]))
            {
                return DataType.Binary;
            }

            if (type == typeof(TimeSpan))
            {
                return DataType.TimeSpan;
            }

            if (type == typeof(DateTime))
            {
                return DataType.DateTime;
            }

            return type.IsEnum ? DataType.Enum : DataType.User;
        }

        /// <summary>Loads the row layout from the specified reader.</summary>
        /// <param name="reader">The reader.</param>
        /// <returns>A new <see cref="RowLayout"/> instance.</returns>
        public static RowLayout Load(DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            var count = reader.Read7BitEncodedInt32();
            var tableName = reader.ReadPrefixedString();
            var fieldProperties = new FieldProperties[count];
            for (var i = 0; i < count; i++)
            {
                var field = fieldProperties[i] = new();
                field.Load(reader, i);
            }

            return new(tableName, fieldProperties, null);
        }

        /// <summary>Enums the value.</summary>
        /// <param name="index">The field index.</param>
        /// <param name="value">The value.</param>
        /// <param name="provider">The format provider.</param>
        /// <returns>The enum value.</returns>
        public object EnumValue(int index, long value, IFormatProvider provider = null)
        {
            var field = fieldProperties[index];
            return Enum.Parse(field.ValueType, value.ToString(provider), true);
        }

        /// <inheritdoc/>
        public bool Equals(RowLayout other)
        {
            if (other is null)
            {
                return false;
            }

            if (other.FieldCount != FieldCount)
            {
                return false;
            }

            for (var i = 0; i < FieldCount; i++)
            {
                if (!other.fieldProperties[i].Equals(fieldProperties[i]))
                {
                    return false;
                }
            }

            return true;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj) => obj is RowLayout layout && Equals(layout);

        /// <summary>Gets the string representing the specified value using the field properties.</summary>
        /// <param name="fieldIndex">The field number.</param>
        /// <param name="value">The value.</param>
        /// <param name="culture">The culture.</param>
        /// <returns>The string to display.</returns>
        public string GetDisplayString(int fieldIndex, object value, CultureInfo culture = null)
        {
            var field = fieldProperties[fieldIndex];
            return field.DisplayFormat switch
            {
                "FormatTimeSpan" or "TimeSpan" => field.DataType switch
                {
                    DataType.TimeSpan => ((TimeSpan)value).FormatTime(),
                    _ => Convert.ToDouble(value, culture).FormatSeconds()
                },
                "FormatValue" or "Size" => Convert.ToDecimal(value, culture).FormatSize(),
                "FormatBinary" or "Binary" => Convert.ToDecimal(value, culture).FormatSize(),
                _ => field.DataType switch
                {
                    DataType.Int8 => ((sbyte)value).ToString(field.DisplayFormat, culture),
                    DataType.Int16 => ((short)value).ToString(field.DisplayFormat, culture),
                    DataType.Int32 => ((int)value).ToString(field.DisplayFormat, culture),
                    DataType.Int64 => ((long)value).ToString(field.DisplayFormat, culture),
                    DataType.UInt8 => ((byte)value).ToString(field.DisplayFormat, culture),
                    DataType.UInt16 => ((ushort)value).ToString(field.DisplayFormat, culture),
                    DataType.UInt32 => ((uint)value).ToString(field.DisplayFormat, culture),
                    DataType.UInt64 => ((ulong)value).ToString(field.DisplayFormat, culture),
                    DataType.Binary => Base64.NoPadding.Encode((byte[])value),
                    DataType.DateTime => ((DateTime)value).ToString(field.DisplayFormat, culture),
                    DataType.Single => ((float)value).ToString(field.DisplayFormat, culture),
                    DataType.Double => ((double)value).ToString(field.DisplayFormat, culture),
                    DataType.Decimal => ((decimal)value).ToString(field.DisplayFormat, culture),
                    _ => value == null ? string.Empty : value.ToString()
                }
            };
        }

        /// <inheritdoc/>
        public IEnumerator<IFieldProperties> GetEnumerator() => fieldProperties.GetEnumerator();

        /// <summary>Gets the field index of the specified field name.</summary>
        /// <param name="fieldName">The field name to search for.</param>
        /// <returns>The field index of the specified field name.</returns>
        [Obsolete("Use int GetFieldIndex(string fieldName, bool throwException) instead!")]
        public int GetFieldIndex(string fieldName) => GetFieldIndex(fieldName, false);

        /// <summary>Gets the field index of the specified field name.</summary>
        /// <param name="fieldName">The field name to search for.</param>
        /// <param name="throwException">Throw exception if field cannot be found.</param>
        /// <returns>The field index of the specified field name.</returns>
        public int GetFieldIndex(string fieldName, bool throwException) => GetFieldIndex(fieldName, 0, throwException);

        /// <summary>Gets the field index of the specified field name.</summary>
        /// <param name="fieldName">The field name to search for.</param>
        /// <param name="comparison">Field name comparison</param>
        /// <param name="throwException">Throw exception if field cannot be found.</param>
        /// <returns>The field index of the specified field name.</returns>
        public int GetFieldIndex(string fieldName, StringComparison comparison, bool throwException)
        {
            if (comparison == 0)
            {
                comparison = StringComparison.Ordinal;
            }

            //test field name
            var field = fieldProperties.SingleOrDefault(f => f.Name.Equals(fieldName, comparison));
            if (field != null)
            {
                return field.Index;
            }

            //test alternate names
            field = fieldProperties.SingleOrDefault(f => true == f.AlternativeNames?.Any(n => n.Equals(field.Name, comparison)));
            if (field != null)
            {
                return field.Index;
            }

            return !throwException ? -1 : throw new ArgumentOutOfRangeException(nameof(fieldName), $"FieldName {fieldName} is not present at layout {this}!");
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            var result = IsTyped ? 0x00001234 : 0x12345678;
            for (var i = 0; i < FieldCount; i++)
            {
                result ^= fieldProperties[i].GetHashCode() ^ i;
            }

            return result;
        }

        /// <summary>Calculates a new <see cref="RowLayout"/> instance by matching the current typed layout to a database layout.</summary>
        /// <param name="baseTableLayout">Database table layout.</param>
        /// <param name="flags">Table flags</param>
        /// <returns>Returns a new <see cref="RowLayout"/> instance</returns>
        public RowLayout GetMatching(RowLayout baseTableLayout, TableFlags flags)
        {
            if (RowType == null)
            {
                throw new InvalidOperationException("GetMatching is only supported on Typed Layouts!");
            }

            var comparison = flags.GetFieldNameComparison();
            var result = new List<IFieldProperties>();
            foreach (var typedField in this)
            {
                var index = baseTableLayout.GetFieldIndex(typedField.NameAtDatabase, false);
                if (index < 0)
                {
                    index = baseTableLayout.GetFieldIndex(typedField.Name, false);
                    if (index < 0)
                    {
                        index = baseTableLayout.GetFieldIndex(typedField.NameAtDatabase, StringComparison.OrdinalIgnoreCase, false);
                        if (index < 0)
                        {
                            index = baseTableLayout.GetFieldIndex(typedField.Name, StringComparison.OrdinalIgnoreCase, false);
                            if (index < 0)
                            {
                                if (flags.HasFlag(TableFlags.IgnoreMissingFields))
                                {
                                    Trace.TraceWarning($"Ignoring missing field {typedField} at table {baseTableLayout}!");
                                    continue;
                                }
                                throw new InvalidDataException($"Field {typedField} cannot be found at table {baseTableLayout}");
                            }
                        }
                    }
                }
                var untypedField = baseTableLayout[index];

                var target = typedField.Clone();
                target.Index = index;
                target.NameAtDatabase = untypedField.NameAtDatabase;
                target.TypeAtDatabase = untypedField.TypeAtDatabase;
                if (target.Flags != untypedField.Flags)
                {
                    Trace.TraceWarning($"Database table flags {untypedField.Flags} do not match typed flags {typedField.Flags}!");
                }
                result.Add(target);
            }

            if (result.Select(i => i.Index).Distinct().Count() != result.Count)
            {
                throw new("Index assignment is not distinct!");
            }

            return new(baseTableLayout.Name, result.OrderBy(i => i.Index).ToArray(), RowType);
        }

        /// <summary>Gets the name of the field with the given number.</summary>
        /// <param name="index">The field index.</param>
        /// <returns>The name of the field.</returns>
        public string GetName(int index) => fieldProperties[index].Name;

        /// <summary>Loads the structure fields into a new <see cref="Row"/> instance.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="item">Structure to read.</param>
        /// <returns>A new row instance.</returns>
        public Row GetRow<TStruct>(TStruct item)
            where TStruct : struct =>
            new(this, GetValues(item), false);

        /// <summary>
        /// Retrieves a string for the specified value. The string may be parsed back to a value using <see cref="ParseValue(int, string, string, IFormatProvider)"/>.
        /// </summary>
        /// <param name="fieldIndex">The field number.</param>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="culture">The culture.</param>
        /// <returns>A string for the value.</returns>
        public string GetString(int fieldIndex, object value, string stringMarker, CultureInfo culture = null)
        {
            var field = fieldProperties[fieldIndex];
            return field.GetString(value, stringMarker, culture);
        }

        /// <summary>Gets the value of a field from the specified struct.</summary>
        /// <param name="index">The field index.</param>
        /// <param name="item">The struct to read the value from.</param>
        /// <returns>The value of the specified field.</returns>
        public object GetValue(int index, object item)
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            return fieldProperties[index].FieldInfo.GetValue(item);
        }

        /// <summary>Gets all values of the struct.</summary>
        /// <param name="item">The struct to get the values from.</param>
        /// <returns>Returns all values of the struct.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public object[] GetValues<TStruct>(TStruct item)
            where TStruct : struct
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            if (RowType != typeof(TStruct))
            {
                throw new InvalidOperationException($"This RowLayout {RowType} does not match structure type {typeof(TStruct)}!");
            }

            var result = new object[MaxIndex + 1];
            for (var i = 0; i < FieldCount; i++)
            {
                var value = fieldProperties[i].FieldInfo.GetValue(item);
                if (value is DateTime dt && (dt.Kind == DateTimeKind.Unspecified))
                {
                    value = new DateTime(dt.Ticks, DateTimeKind.Local);
                }
                result[fieldProperties[i].Index] = value;
            }

            return result;
        }

        /// <summary>Checks whether a field with the specified name exists or not.</summary>
        /// <param name="fieldName">The field name.</param>
        /// <returns>True is the field exists.</returns>
        public bool HasField(string fieldName) => GetFieldIndex(fieldName, 0, false) > -1;

        /// <summary>Checks whether a field with the specified name exists or not.</summary>
        /// <param name="fieldName">The field name.</param>
        /// <param name="comparison">Field name comparison</param>
        /// <returns>True is the field exists.</returns>
        public bool HasField(string fieldName, StringComparison comparison) => GetFieldIndex(fieldName, comparison, false) > -1;

        /// <summary>Parses the value.</summary>
        /// <param name="index">The field index.</param>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="provider">The format provider.</param>
        /// <returns>The value parsed.</returns>
        public object ParseValue(int index, string value, string stringMarker, IFormatProvider provider = null)
        {
            var field = fieldProperties[index];
            return field.ParseValue(value, stringMarker, provider);
        }

        /// <summary>Creates a copy of this layout without the specified field.</summary>
        /// <param name="fieldName">Name of the field to remove.</param>
        /// <returns>Returns a new <see cref="RowLayout"/> instance.</returns>
        public RowLayout Remove(string fieldName)
        {
            var index = GetFieldIndex(fieldName, true);
            var fieldProperties = new List<IFieldProperties>();
            for (var i = 0; i < FieldCount; i++)
            {
                if (i == index)
                {
                    continue;
                }

                fieldProperties.Add(this.fieldProperties[i]);
            }

            return new(Name, fieldProperties.ToArray(), RowType);
        }

        /// <summary>Saves the layout to the specified writer.</summary>
        /// <param name="writer">The writer.</param>
        public void Save(DataWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            writer.Write7BitEncoded32(FieldCount);
            writer.WritePrefixed(Name);
            for (var i = 0; i < FieldCount; i++)
            {
                Save(writer, fieldProperties[i]);
            }
        }

        /// <summary>Sets a value at the specified struct.</summary>
        /// <param name="index">The field index.</param>
        /// <param name="target">The struct to set the value at.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="culture">Culture to use to convert values.</param>
        [Obsolete("Use overload with IFieldProperties definition!")]
        public void SetValue(int index, ref object target, object value, CultureInfo culture = null) => SetValue(ref target, fieldProperties[index], value, culture);

        /// <summary>Sets a value at the specified struct.</summary>
        /// <param name="target">The struct to set the value at.</param>
        /// <param name="field">Field properties to use</param>
        /// <param name="value">The value to set.</param>
        /// <param name="culture">Culture to use to convert values.</param>
        public void SetValue(ref object target, IFieldProperties field, object value, CultureInfo culture = null)
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            SetValueInternal(ref target, field, value, culture);
        }

        /// <summary>Sets all values of the struct.</summary>
        /// <param name="target">The struct to set the values at.</param>
        /// <param name="values">The values to set.</param>
        /// <param name="culture">Culture to use when converting values.</param>
        public void SetValues(ref object target, object[] values, CultureInfo culture = null)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            for (var i = 0; i < FieldCount; i++)
            {
                var field = fieldProperties[i];
                SetValueInternal(ref target, field, values[field.Index], culture);
            }
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => fieldProperties.GetEnumerator();

        #endregion Public Methods
    }
}
