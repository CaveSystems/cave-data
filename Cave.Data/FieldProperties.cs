using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Cave.Data;
using Cave.IO;
using Cave.Security;

namespace Cave.Data;

/// <summary>Provides field properties.</summary>
public class FieldProperties : IFieldProperties
{
    #region Private Fields

    static readonly char[] NameSeparator = [';', ',', '\t', ' '];

    ConstructorInfo? constructor;
    bool parserInitialized;
    MethodInfo? staticParse;

    #endregion Private Fields

    #region Public Properties

    /// <summary>Empty or invalid field. Used for new instances with lazy initialization.</summary>
    public static FieldProperties None { get; } = new();

    /// <inheritdoc/>
    public IList<string> AlternativeNames { get; set; } = [];

    /// <inheritdoc/>
    public DataType DataType { get; set; }

    /// <inheritdoc/>
    public DateTimeKind DateTimeKind { get; set; }

    /// <inheritdoc/>
    public DateTimeType DateTimeType { get; set; }

    /// <inheritdoc/>
    public object? DefaultValue { get; set; }

    /// <inheritdoc/>
    public string? Description { get; set; }

    /// <inheritdoc/>
    public string? DisplayFormat { get; set; }

    /// <inheritdoc/>
    public string DotNetTypeName
    {
        get
        {
            switch (DataType)
            {
                case DataType.Binary: return "byte[]";
                case DataType.Bool: return "bool";
                case DataType.DateTime: return "DateTime";
                case DataType.Decimal: return "decimal";
                case DataType.Double: return "double";
                case DataType.Int16: return "short";
                case DataType.Int32: return "int";
                case DataType.Int64: return "long";
                case DataType.Int8: return "sbyte";
                case DataType.Single: return "float";
                case DataType.String: return "string";
                case DataType.TimeSpan: return "TimeSpan";
                case DataType.UInt16: return "ushort";
                case DataType.UInt32: return "uint";
                case DataType.UInt64: return "ulong";
                case DataType.UInt8: return "byte";
                case DataType.Char: return "char";
                default:
                    // case DataType.User: case DataType.Enum:
                    if (ValueType != null)
                    {
                        return ValueType.Name;
                    }

                    return $"unknown datatype {DataType}";
            }
        }
    }

    /// <inheritdoc/>
    public FieldInfo? FieldInfo { get; set; }

    /// <inheritdoc/>
    public FieldFlags Flags { get; set; }

    /// <inheritdoc/>
    public int Index { get; set; }

    /// <inheritdoc/>
    public bool IsNullable => Flags.HasFlag(FieldFlags.Nullable);

    /// <inheritdoc/>
    public float MaximumLength { get; set; }

    /// <inheritdoc/>
    public string Name { get; set; } = string.Empty;

    string? nameAtDatabase;

    /// <inheritdoc/>
    public string NameAtDatabase
    {
        get => nameAtDatabase ?? Name;
        set => nameAtDatabase = value;
    }

    /// <inheritdoc/>
    public StringEncoding StringEncoding { get; set; }

    /// <inheritdoc/>
    public DataType TypeAtDatabase { get; set; }

    /// <inheritdoc/>
    public Type? ValueType { get; set; }

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public FieldProperties Clone() => new()
    {
        Index = Index,
        ValueType = ValueType,
        DataType = DataType,
        Flags = Flags,
        Name = Name,
        NameAtDatabase = NameAtDatabase,
        TypeAtDatabase = TypeAtDatabase,
        DateTimeKind = DateTimeKind,
        DateTimeType = DateTimeType,
        StringEncoding = StringEncoding,
        MaximumLength = MaximumLength,
        Description = Description,
        DisplayFormat = DisplayFormat,
        AlternativeNames = AlternativeNames,
        FieldInfo = FieldInfo,
        DefaultValue = DefaultValue
    };

    /// <inheritdoc/>
    public object EnumValue(long value)
    {
        if (ValueType == null)
        {
            throw new InvalidOperationException("This function requires a valid ValueType!");
        }

        // handle enum only
        if (DataType != DataType.Enum)
        {
            throw new ArgumentException("DataType is not an enum!");
        }

        return Enum.ToObject(ValueType, value);
    }

    /// <summary>Checks another FieldProperties instance for equality.</summary>
    /// <param name="other">The FieldProperties to check for equality.</param>
    /// <param name="fieldNameComparison">StringComparison to be used for fieldnames.</param>
    /// <returns>Returns true if the other instance equals this one, false otherwise.</returns>
    public bool IsCompatible(IFieldProperties? other, StringComparison fieldNameComparison)
    {
        if (other == null)
        {
            return false;
        }

        // check name
        if (!string.Equals(other.Name, Name, fieldNameComparison) && !string.Equals(other.NameAtDatabase, NameAtDatabase, fieldNameComparison))
        {
            var nameMatching =
                AlternativeNames?.Any(n => string.Equals(n, other.Name, fieldNameComparison) || string.Equals(n, other.NameAtDatabase, fieldNameComparison)) ??
                other.AlternativeNames?.Any(n => string.Equals(n, Name, fieldNameComparison) || string.Equals(n, NameAtDatabase, fieldNameComparison));
            if (nameMatching != true) return false;
        }

        if ((other.FieldInfo != null) && (FieldInfo != null))
        {
            // both typed, full match needed
            return other.ValueType == ValueType;
        }

        if (DataType == DataType.User) return other.DataType is DataType.User or DataType.String;
        if (other.DataType == DataType.User) return DataType is DataType.User or DataType.String;
        if (DataType == other.DataType)
        {
            return true;
        }

        Trace.WriteLine($"FieldProperties not compatible: {this} != {other}");
        return false;
    }

    /// <summary>Gets the hashcode for the instance.</summary>
    /// <returns>Hashcode for the field.</returns>
    public override int GetHashCode() => ToString().GetHashCode();

    /// <inheritdoc/>
    public string GetString(object value, string? stringMarker = null, IFormatProvider? provider = null) => Fields.GetString(value, DataType, DateTimeKind, DateTimeType, stringMarker, provider);

    /// <summary>Loads fieldproperties from the specified reader.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">Field index.</param>
    public void Load(DataReader reader, int index)
    {
        if (reader == null)
        {
            throw new ArgumentNullException(nameof(reader));
        }

        DataType = (DataType)reader.Read7BitEncodedInt32();
        TypeAtDatabase = (DataType)reader.Read7BitEncodedInt32();
        Flags = (FieldFlags)reader.Read7BitEncodedInt32();
        Name = reader.ReadPrefixedString() ?? $"Field{Base32.Safe.Encode(RNG.UInt32)}";
        NameAtDatabase = reader.ReadPrefixedString() ?? Name;
        Index = index;
        var typeName = reader.ReadPrefixedString() ?? throw new InvalidDataException($"Unknown type at field {DataType} {Name}!");
        ValueType = Type.GetType(typeName, true) ?? throw new InvalidDataException($"Unknown type {typeName} at field {DataType} {Name}!");
        if (DataType == DataType.DateTime)
        {
            DateTimeKind = (DateTimeKind)reader.Read7BitEncodedInt32();
            DateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
        }

        if (DataType is DataType.String or DataType.User)
        {
            MaximumLength = reader.ReadSingle();
        }

        Validate();
    }

    /// <summary>Loads field properties using the specified FieldInfo.</summary>
    /// <param name="index">Field index.</param>
    /// <param name="fieldInfo">The field information.</param>
    /// <exception cref="NotSupportedException">Array types (except byte[]) are not supported!.</exception>
    public void LoadFieldInfo(int index, FieldInfo fieldInfo) => LoadFieldInfo(index, fieldInfo, NamingStrategy.Exact);

    /// <summary>Loads field properties using the specified FieldInfo.</summary>
    /// <param name="index">Field index.</param>
    /// <param name="fieldInfo">The field information.</param>
    /// <param name="namingStrategy">Naming strategy used for building the database field names.</param>
    /// <exception cref="NotSupportedException">Array types (except byte[]) are not supported!.</exception>
    public void LoadFieldInfo(int index, FieldInfo fieldInfo, NamingStrategy namingStrategy)
    {
        FieldInfo = fieldInfo ?? throw new ArgumentNullException(nameof(fieldInfo));
        Index = index;
        Name = fieldInfo.Name;
        Flags = FieldFlags.None;

        if (fieldInfo.FieldType.IsGenericType)
        {
            var nullableType = Nullable.GetUnderlyingType(fieldInfo.FieldType);
            if (nullableType != null)
            {
                Flags |= FieldFlags.Nullable;
                ValueType = nullableType;
            }
        }

        ValueType ??= fieldInfo.FieldType;
        DataType = RowLayout.DataTypeFromType(ValueType);
        MaximumLength = 0;
        DisplayFormat = null;
        Description = null;
        DateTimeKind = DateTimeKind.Unspecified;
        DateTimeType = DateTimeType.Undefined;
        StringEncoding = StringEncoding.Undefined;
        AlternativeNames = [];
        TypeAtDatabase = DataType switch
        {
            DataType.Enum => DataType.Int64,
            DataType.User => !fieldInfo.FieldType.IsArray ? DataType.String : throw new NotSupportedException("Array types (except byte[]) are not supported!\nPlease define a class with a valid ToString() member and static Parse(string) constructor instead!"),
            _ => DataType
        };

        var defaultValueOverride = false;
        foreach (var attribute in fieldInfo.GetCustomAttributes(false))
        {
            if (attribute is FieldAttribute fieldAttribute)
            {
                MaximumLength = fieldAttribute.Length;
                if (fieldAttribute.Name != null)
                {
                    NameAtDatabase = fieldAttribute.Name;
                }

                Flags |= fieldAttribute.Flags;
                DisplayFormat = fieldAttribute.DisplayFormat;
                AlternativeNames = fieldAttribute.AlternativeNames?.Split(NameSeparator, StringSplitOptions.RemoveEmptyEntries) ?? [];
                continue;
            }

            if (attribute is DescriptionAttribute descriptionAttribute)
            {
                Description = descriptionAttribute.Description;
                continue;
            }

            if (attribute is DateTimeFormatAttribute dateTimeFormatAttribute)
            {
                DateTimeKind = dateTimeFormatAttribute.Kind;
                DateTimeType = dateTimeFormatAttribute.Type;
                TypeAtDatabase = DateTimeType switch
                {
                    DateTimeType.BigIntMilliSeconds or DateTimeType.BigIntSeconds or DateTimeType.BigIntEpoch or DateTimeType.BigIntTicks or DateTimeType.BigIntHumanReadable => DataType.Int64,
                    DateTimeType.DecimalSeconds => DataType.Decimal,
                    DateTimeType.DoubleSeconds or DateTimeType.DoubleEpoch => DataType.Double,
                    DateTimeType.Undefined or DateTimeType.Native => DataType.DateTime,
                    _ => throw new NotImplementedException($"DateTimeType {DateTimeType} is not implemented!"),
                };
                continue;
            }

            if (attribute is TimeSpanFormatAttribute timeSpanFormatAttribute)
            {
                DateTimeType = timeSpanFormatAttribute.Type;
                TypeAtDatabase = DateTimeType switch
                {
                    DateTimeType.BigIntMilliSeconds or DateTimeType.BigIntSeconds or DateTimeType.BigIntEpoch or DateTimeType.BigIntTicks or DateTimeType.BigIntHumanReadable => DataType.Int64,
                    DateTimeType.DecimalSeconds => DataType.Decimal,
                    DateTimeType.DoubleSeconds => DataType.Double,
                    DateTimeType.Undefined or DateTimeType.Native => DataType.TimeSpan,
                    _ => throw new NotImplementedException($"DateTimeType {DateTimeType} is not implemented!"),
                };
                continue;
            }

            if (attribute is StringFormatAttribute stringFormatAttribute)
            {
                StringEncoding = stringFormatAttribute.Encoding;
                continue;
            }

            if (attribute is DefaultValueAttribute defaultValueAttribute)
            {
                defaultValueOverride = true;
                DefaultValue = defaultValueAttribute.Value;
                continue;
            }

            if (attribute.GetType().Name == "NullableAttribute")
            {
                Flags |= FieldFlags.Nullable;
                continue;
            }

            Debugger.Break();
        }

        if (TypeAtDatabase == 0)
        {
            TypeAtDatabase = DataType;
        }

        if (!IsNullable && !defaultValueOverride)
        {
            DefaultValue ??= Fields.GetDefault(ValueType);
        }
        NameAtDatabase ??= namingStrategy.GetNameByStrategy(fieldInfo.Name);

        Validate();
    }

    /// <inheritdoc/>
    public object? ParseValue(string text, string? stringMarker = null, IFormatProvider? provider = null)
    {
        provider ??= CultureInfo.InvariantCulture;

        if (ValueType == null)
        {
            throw new InvalidOperationException("This function requires a valid ValueType!");
        }

        if (text == null)
        {
            return null;
        }

        if (stringMarker != null)
        {
            if (text == "null")
            {
                return null;
            }
        }

        switch (DataType)
        {
            case DataType.TimeSpan:
            {
                if (string.IsNullOrEmpty(text) || (text == "null"))
                {
                    return IsNullable ? null : default(TimeSpan);
                }

                switch (DateTimeType)
                {
                    case DateTimeType.BigIntEpoch:
                    case DateTimeType.DoubleEpoch:
                    default: throw new NotSupportedException($"DateTimeType {DateTimeType} is not supported.");

                    case DateTimeType.BigIntHumanReadable:
                        return new TimeSpan(DateTime.ParseExact(text, Storage.BigIntDateTimeFormat, provider).Ticks);

                    case DateTimeType.Undefined:
                    case DateTimeType.Native:
                        if (stringMarker != null)
                        {
                            text = text.Unbox(stringMarker, false);
                        }
#if NET20 || NET35
                        return TimeSpan.Parse(text);
#else
                        return TimeSpan.Parse(text, provider);
#endif
                    case DateTimeType.BigIntTicks:
                        return new TimeSpan(long.Parse(text, provider));

                    case DateTimeType.BigIntMilliSeconds:
                        return new TimeSpan(long.Parse(text, provider) * TimeSpan.TicksPerMillisecond);

                    case DateTimeType.BigIntSeconds:
                        return new TimeSpan(long.Parse(text, provider) * TimeSpan.TicksPerSecond);

                    case DateTimeType.DecimalSeconds:
                        return new TimeSpan((long)decimal.Round(decimal.Parse(text, provider) * TimeSpan.TicksPerSecond));

                    case DateTimeType.DoubleSeconds:
                    {
                        var value = double.Parse(text, provider) * TimeSpan.TicksPerSecond;
                        var longValue = (long)value;
                        if ((value > 0) && (longValue < 0))
                        {
                            Trace.WriteLine("DoubleSeconds exceeded (long) range. Overflow detected!");
                            longValue = long.MaxValue;
                        }
                        else if ((value < 0) && (longValue > 0))
                        {
                            Trace.WriteLine("DoubleSeconds exceeded (long) range. Overflow detected!");
                            longValue = long.MinValue;
                        }

                        return new TimeSpan(longValue);
                    }
                }
            }
            case DataType.DateTime:
            {
                if (string.IsNullOrEmpty(text) || (text == "null"))
                {
                    return IsNullable ? null : default(DateTime);
                }

                switch (DateTimeType)
                {
                    default: throw new NotSupportedException($"DateTimeType {DateTimeType} is not supported.");
                    case DateTimeType.BigIntHumanReadable:
                        return DateTime.ParseExact(text, Storage.BigIntDateTimeFormat, provider);

                    case DateTimeType.Undefined:
                    case DateTimeType.Native:
                        if (stringMarker != null)
                        {
                            text = text.Unbox(stringMarker, false);
                        }

                        return DateTime.ParseExact(text, StringExtensions.InteropDateTimeFormat, provider);

                    case DateTimeType.BigIntTicks:
                        return new DateTime(long.Parse(text, provider), DateTimeKind);

                    case DateTimeType.DecimalSeconds:
                        return new DateTime((long)decimal.Round(decimal.Parse(text, provider) * TimeSpan.TicksPerSecond), DateTimeKind);

                    case DateTimeType.DoubleSeconds:
                        return new DateTime((long)Math.Round(double.Parse(text, provider) * TimeSpan.TicksPerSecond), DateTimeKind);

                    case DateTimeType.DoubleEpoch:
                        return new DateTime((long)Math.Round(double.Parse(text, provider) * TimeSpan.TicksPerSecond) + Storage.EpochTicks, DateTimeKind);

                    case DateTimeType.BigIntEpoch:
                        return new DateTime((long.Parse(text, provider) * TimeSpan.TicksPerSecond) + Storage.EpochTicks, DateTimeKind);

                    case DateTimeType.BigIntMilliSeconds:
                        return new DateTime(long.Parse(text, provider) * TimeSpan.TicksPerMillisecond, DateTimeKind);

                    case DateTimeType.BigIntSeconds:
                        return new DateTime(long.Parse(text, provider) * TimeSpan.TicksPerSecond, DateTimeKind);
                }
            }
            case DataType.Binary:
            {
                if (string.IsNullOrEmpty(text) || (text == "null"))
                {
                    return null;
                }

                if (stringMarker != null)
                {
                    text = text.Unbox(stringMarker, false);
                }

                return Base64.NoPadding.Decode(text);
            }
            case DataType.Bool:
                if (text.Length == 0)
                {
                    return IsNullable ? null : false;
                }

                return (text.ToUpperInvariant() == "TRUE") || (text.ToUpperInvariant() == "YES") || (text == "1");

            case DataType.Single:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0f;
                }

                return float.Parse(text, provider);

            case DataType.Double:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0d;
                }

                return double.Parse(text, provider);

            case DataType.Decimal:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0m;
                }

                return decimal.Parse(text, provider);

            case DataType.Int8:
                if (text.Length == 0)
                {
                    return IsNullable ? null : (sbyte)0;
                }

                return sbyte.Parse(text, provider);

            case DataType.Int16:
                if (text.Length == 0)
                {
                    return IsNullable ? null : (short)0;
                }

                return short.Parse(text, provider);

            case DataType.Int32:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0;
                }

                return int.Parse(text, provider);

            case DataType.Int64:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0L;
                }

                return long.Parse(text, provider);

            case DataType.UInt8:
                if (text.Length == 0)
                {
                    return IsNullable ? null : (byte)0;
                }

                return byte.Parse(text, provider);

            case DataType.UInt16:
                if (text.Length == 0)
                {
                    return IsNullable ? null : (ushort)0;
                }

                return ushort.Parse(text, provider);

            case DataType.UInt32:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0U;
                }

                return uint.Parse(text, provider);

            case DataType.UInt64:
                if (text.Length == 0)
                {
                    return IsNullable ? null : 0UL;
                }

                return ulong.Parse(text, provider);

            case DataType.Enum:
                if (stringMarker != null)
                {
                    text = text.Unbox(stringMarker, false);
                }

                if (text.Length == 0)
                {
                    text = "0";
                }

                return Enum.Parse(ValueType, text, true);

            case DataType.Guid:
                if (stringMarker != null)
                {
                    text = text.Unbox(stringMarker, false);
                }

                if (text.Length == 0)
                {
                    return null;
                }

                return new Guid(text);

            case DataType.Char:
                if (stringMarker != null)
                {
                    text = text.Unbox(stringMarker, false).Unescape();
                }

                if (text.Length != 1)
                {
                    throw new InvalidDataException();
                }

                return text[0];

            case DataType.String:
                if (stringMarker != null)
                {
                    text = text.Unbox(stringMarker, false).Unescape();
                }

                return text;

            case DataType.User: break;
            default: throw new NotImplementedException();
        }

        if (stringMarker != null)
        {
            text = text.Unbox(stringMarker, false).Unescape();
        }

        if (!parserInitialized)
        {
            // lookup static Parse(string) method first
            staticParse = ValueType.GetMethod("Parse", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static, null, [typeof(string)], null);

            // if there is none, search constructor(string)
            if (staticParse == null)
            {
                constructor = ValueType.GetConstructor([typeof(string)]);
            }

            parserInitialized = true;
        }

        // has static Parse(string) ?
        if (staticParse != null)
        {
            // use method to parse value
            return staticParse.Invoke(null, new object[] { text });
        }

        // has constructor(string) ?
        if (constructor != null)
        {
            return constructor.Invoke(new object[] { text });
        }

        throw new MissingMethodException($"Could not find a way to parse or create {ValueType} from string!");
    }

    /// <summary>Returns a <see cref="string"/> that represents this instance.</summary>
    /// <returns>A <see cref="string"/> that represents this instance.</returns>
    public override string ToString()
    {
        var result = new StringBuilder();
        if (Flags != 0)
        {
            result.Append($"[{Flags}] ");
        }

        result.Append(DotNetTypeName);
        if (IsNullable)
        {
            result.Append("? ");
        }

        if (StringEncoding != default)
        {
            result.Append($" {StringEncoding}");
        }

        if (DateTimeType != default)
        {
            result.Append($" {DateTimeType}");
        }

        if (DateTimeKind != default)
        {
            result.Append($" {DateTimeKind}");
        }

        result.Append($" {Name}");
        if (MaximumLength is > 0 and < int.MaxValue)
        {
            result.Append($" ({MaximumLength})");
        }

        return result.ToString();
    }

    /// <summary>Checks properties and sets needed but unset settings.</summary>
    /// <returns>Returns a reference to this instance.</returns>
    public IFieldProperties Validate()
    {
        if (TypeAtDatabase == 0) { TypeAtDatabase = DataType; }

        NameAtDatabase ??= Name;

        switch (DataType)
        {
            case DataType.Binary:
            case DataType.Bool:
            case DataType.Decimal:
            case DataType.Double:
            case DataType.Int8:
            case DataType.Int16:
            case DataType.Int32:
            case DataType.Int64:
            case DataType.UInt8:
            case DataType.UInt16:
            case DataType.UInt32:
            case DataType.UInt64:
            case DataType.Char:
            case DataType.Single:
                break;

            case DataType.TimeSpan:
            case DataType.DateTime:
                if (DateTimeType == DateTimeType.Undefined)
                {
                    DateTimeType = DateTimeType.Native;
#if DEBUG
                    Trace.TraceWarning("Field {0} DateTimeType undefined! Falling back to native date time type. (Precision may be only seconds!)", this);
#endif
                }

                break;

            case DataType.Enum:
                if (ValueType == null)
                {
                    throw new InvalidOperationException($"Property {nameof(ValueType)} required!");
                }

                if (TypeAtDatabase == DataType.Enum)
                {
                    TypeAtDatabase = DataType.Int64;
#if DEBUG
                    Trace.TraceWarning("Field {0} DatabaseDataType undefined! Using DatabaseDataType {1}!", this, TypeAtDatabase);
#endif
                }

                break;

            case DataType.Guid:
                switch (TypeAtDatabase)
                {
                    case DataType.String:
                    case DataType.Guid:
                        break;

                    default:
                        throw new NotImplementedException($"DataType Guid cannot use underlying DataType {TypeAtDatabase}!");
                }

                break;

            case DataType.String:
                if (StringEncoding == StringEncoding.Undefined)
                {
                    StringEncoding = StringEncoding.UTF_8;
                }

                break;

            case DataType.User:
                if (ValueType == null)
                {
                    throw new InvalidOperationException($"Property {nameof(ValueType)} required!");
                }

                switch (TypeAtDatabase)
                {
                    case DataType.User:
                        TypeAtDatabase = DataType.String;
                        Trace.TraceWarning("Field {0} DatabaseDataType undefined! Using DatabaseDataType {1}!", this, TypeAtDatabase);
                        if (StringEncoding == StringEncoding.Undefined)
                        {
                            StringEncoding = StringEncoding.UTF_8;
                        }

                        break;

                    case DataType.String:
                        if (StringEncoding == StringEncoding.Undefined)
                        {
                            StringEncoding = StringEncoding.UTF_8;
                        }

                        break;

                    default: throw new NotSupportedException($"Datatype {TypeAtDatabase} is not supported for field {this}!");
                }

                goto case DataType.String;
            default:
                throw new NotImplementedException("Unknown DataType!");
        }

        return this;
    }

    #endregion Public Methods
}
