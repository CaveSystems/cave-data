﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Cave.Collections.Generic;

#nullable enable

namespace Cave.Data
{
    /// <summary>Code generator for ITable or RowLayout instances.</summary>
    public class TableInterfaceGenerator
    {
        /// <summary>
        /// The namespace to use for the class (defaults to <see cref="DatabaseName"/>). Optional. Default = "Database".
        /// </summary>
        public string? NameSpace { get; set; }

        /// <summary>
        /// Gets or sets the database name (only used for the structure name). Required!
        /// </summary>
        public string? DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the table name (only used for the structure name). Optional. Default = <see cref="Layout"/>.Name.
        /// </summary>
        public string? TableName { get; set; }

        /// <summary>
        /// Gets or sets the name of the class to generate. Optional.
        /// </summary>
        public string? ClassName { get; set; }

        /// <summary>
        /// Gets or sets the naming strategy for classes, properties, structures and fields. Optional. Default = <see cref="NamingStrategy.PascalCase"/>.
        /// </summary>
        public NamingStrategy NamingStrategy { get; set; } = NamingStrategy.PascalCase;

        /// <summary>
        /// Gets or sets the <see cref="RowLayout"/> to be used for the structure. Required!
        /// </summary>
        public RowLayout? Layout { get; set; }

        /// <returns>Returns a string containing csharp code.</returns>
        public GenerateTableCodeResult GenerateStruct()
        {
            if (Layout == null)
            {
                throw new ArgumentNullException(nameof(Layout));
            }
            var generator = typeof(TableInterfaceGenerator);

            string GetName(string text) => NamingStrategy.GetNameByStrategy(text);

            if (DatabaseName == null)
            {
                throw new ArgumentNullException(nameof(DatabaseName));
            }

            NameSpace ??= "Database";
            TableName ??= Layout.Name;

            var fieldNameLookup = new Dictionary<int, string>();
            var idCount = Layout.Identifier.Count();
            var idFields = (idCount == 0 ? Layout : Layout.Identifier).ToList();
            var code = new StringBuilder();
            code.AppendLine("//-----------------------------------------------------------------------");
            code.AppendLine("// <summary>");
            code.AppendLine("// Autogenerated table class");
            code.AppendLine("// </summary>");
            code.AppendLine("// <auto-generated />");
            code.AppendLine("//-----------------------------------------------------------------------");
            code.AppendLine();
            code.AppendLine("using System;");
            code.AppendLine("using System.Globalization;");
            code.AppendLine("using System.CodeDom.Compiler;");
            code.AppendLine("using Cave.Data;");

            #region Build lookup tables

            void BuildLookupTables()
            {
                var uniqueFieldNames = new IndexedSet<string>();
                foreach (var field in Layout)
                {
                    var sharpName = GetName(field.Name);
                    var i = 0;
                    while (uniqueFieldNames.Contains(sharpName))
                    {
                        sharpName = GetName(field.Name) + ++i;
                    }

                    uniqueFieldNames.Add(sharpName);
                    fieldNameLookup[field.Index] = sharpName;
                }
            }

            BuildLookupTables();

            #endregion

            ClassName ??= GetName(DatabaseName) + GetName(TableName) + "Row";

            code.AppendLine();
            code.AppendLine($"namespace {NameSpace}");
            code.AppendLine("{");
            code.AppendLine($"\t/// <summary>Table structure for {Layout.Name}.</summary>");
            code.AppendLine($"\t[GeneratedCode(\"{generator.FullName}\",\"{generator.Assembly.GetName().Version}\")]");
            code.AppendLine($"\t[Table(\"{Layout.Name}\")]");
            code.AppendLine($"\tpublic partial struct {ClassName} : IEquatable<{ClassName}>");
            code.AppendLine("\t{");

            #region static Parse()

            code.AppendLine($"\t\t/// <summary>Converts the string representation of a row to its {ClassName} equivalent.</summary>");
            code.AppendLine("\t\t/// <param name=\"data\">A string that contains a row to convert.</param>");
            code.AppendLine($"\t\t/// <returns>A new {ClassName} instance.</returns>");
            code.AppendLine($"\t\tpublic static {ClassName} Parse(string data) => Parse(data, CultureInfo.InvariantCulture);");
            code.AppendLine();
            code.AppendLine($"\t\t/// <summary>Converts the string representation of a row to its {ClassName} equivalent.</summary>");
            code.AppendLine("\t\t/// <param name=\"data\">A string that contains a row to convert.</param>");
            code.AppendLine("\t\t/// <param name=\"provider\">The format provider (optional).</param>");
            code.AppendLine($"\t\t/// <returns>A new {ClassName} instance.</returns>");
            code.AppendLine($"\t\tpublic static {ClassName} Parse(string data, IFormatProvider provider) => CsvReader.ParseRow<{ClassName}>(data, provider);");

            #endregion

            #region Add fields

            foreach (var field in Layout)
            {
                code.AppendLine();
                code.AppendLine($"\t\t/// <summary>{field} {field.Description}.</summary>");
                if (!string.IsNullOrEmpty(field.Description))
                {
                    code.AppendLine($"\t\t[Description(\"{field} {field.Description}\")]");
                }

                code.Append("\t\t[Field(");
                var i = 0;

                void AddAttribute<T>(T value, Func<string> content)
                {
                    if (Equals(value, default))
                    {
                        return;
                    }

                    if (i++ > 0)
                    {
                        code.Append(", ");
                    }

                    code.Append(content());
                }

                if (field.Flags != 0)
                {
                    code.Append("Flags = ");
                    var flagCount = 0;
                    foreach (var flag in field.Flags.GetFlags())
                    {
                        if (flagCount++ > 0)
                        {
                            code.Append(" | ");
                        }

                        code.Append("FieldFlags.");
                        code.Append(flag);
                    }

                    code.Append(", ");
                }

                var sharpName = fieldNameLookup[field.Index];
                if (sharpName != field.Name)
                {
                    AddAttribute(field.Name, () => $"Name = \"{field.Name}\"");
                }

                if (field.MaximumLength < int.MaxValue)
                {
                    AddAttribute(field.MaximumLength, () => $"Length = {(int)field.MaximumLength}");
                }

                AddAttribute(field.AlternativeNames, () => $"AlternativeNames = \"{field.AlternativeNames.Join(", ")}\"");
                AddAttribute(field.DisplayFormat, () => $"DisplayFormat = \"{field.DisplayFormat.EscapeUtf8()}\"");
                code.AppendLine(")]");
                if ((field.DateTimeKind != DateTimeKind.Unspecified) || (field.DateTimeType != DateTimeType.Undefined))
                {
                    code.AppendLine($"\t\t[DateTimeFormat({field.DateTimeKind}, {field.DateTimeType})]");
                }

                if (field.StringEncoding != 0)
                {
                    code.AppendLine($"\t\t[Cave.IO.StringFormat(Cave.IO.StringEncoding.{field.StringEncoding})]");
                }

                code.AppendLine($"\t\tpublic {field.DotNetTypeName} {sharpName};");
            }

            #endregion

            #region ToString()

            {
                code.AppendLine();
                code.AppendLine("\t\t/// <summary>Gets a string representation of this row.</summary>");
                code.AppendLine("\t\t/// <returns>Returns a string that can be parsed by <see cref=\"Parse(string)\"/>.</returns>");
                code.AppendLine("\t\tpublic override string ToString() => ToString(CultureInfo.InvariantCulture);");
                code.AppendLine();
                code.AppendLine("\t\t/// <summary>Gets a string representation of this row.</summary>");
                code.AppendLine("\t\t/// <returns>Returns a string that can be parsed by <see cref=\"Parse(string, IFormatProvider)\"/>.</returns>");
                code.AppendLine("\t\tpublic string ToString(IFormatProvider provider) => CsvWriter.RowToString(this, provider);");
            }

            #endregion

            #region GetHashCode()

            {
                code.AppendLine();
                if (idCount == 1)
                {
                    var idField = Layout.Identifier.First();
                    var idFieldName = fieldNameLookup[idField.Index];
                    code.AppendLine($"\t\t/// <summary>Gets the hash code for the identifier of this row (field {idFieldName}).</summary>");
                    code.AppendLine("\t\t/// <returns>A hash code for the identifier of this row.</returns>");
                    code.Append("\t\tpublic override int GetHashCode() => ");
                    code.Append(idFieldName);
                    code.AppendLine(".GetHashCode();");
                }
                else
                {
                    if (idCount == 0)
                    {
                        code.AppendLine("\t\t/// <summary>Gets the hash code based on all values of this row (no identififer defined).</summary>");
                    }
                    else
                    {
                        var names = idFields.Select(field => fieldNameLookup[field.Index]).Join(", ");
                        code.AppendLine($"\t\t/// <summary>Gets the hash code for the identifier of this row (fields {names}).</summary>");
                    }

                    code.AppendLine("\t\t/// <returns>A hash code for the identifier of this row.</returns>");
                    code.AppendLine("\t\tpublic override int GetHashCode() =>");
                    var first = true;
                    foreach (var idField in idFields)
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            code.AppendLine(" ^");
                        }

                        code.Append($"\t\t\t{fieldNameLookup[idField.Index]}.GetHashCode()");
                    }

                    code.AppendLine(";");
                }
            }

            #endregion

            #region Equals()

            {
                code.AppendLine();
                code.AppendLine("\t\t/// <inheritdoc/>");
                code.AppendLine($"\t\tpublic override bool Equals(object other) => other is {ClassName} row && Equals(row);");
                code.AppendLine();
                code.AppendLine("\t\t/// <inheritdoc/>");
                code.AppendLine($"\t\tpublic bool Equals({ClassName} other)");
                code.AppendLine("\t\t{");
                code.AppendLine("\t\t\treturn");
                {
                    var first = true;
                    foreach (var field in Layout)
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            code.AppendLine(" &&");
                        }

                        var name = fieldNameLookup[field.Index];
                        code.Append($"\t\t\t\tEquals(other.{name}, {name})");
                    }

                    code.AppendLine(";");
                }
                code.AppendLine("\t\t}");
            }

            #endregion

            code.AppendLine("\t}");
            code.AppendLine("}");
            code.Replace("\t", "    ");
            return new()
            {
                ClassName = ClassName,
                TableName = TableName,
                DatabaseName = DatabaseName,
                Code = code.ToString()
            };
        }

        /// <summary>Builds the csharp code file containing the row layout structure.</summary>
        /// <returns>Returns the generated code.</returns>
        public GenerateTableCodeResult GenerateStructFile(string? structFileName = null)
        {
            var result = GenerateStruct();
            if (result.FileName == null)
            {
                if (structFileName == null)
                {
                    result.FileName = result.ClassName + ".cs";
                }
                else
                {
                    result.FileName = structFileName;
                }
            }
            return SaveStructFile(result);
        }

        /// <summary>Saves the generated code to a file and returns the updated result.</summary>
        /// <param name="result">Result to update.</param>
        /// <returns>Returns the generated code.</returns>
        public GenerateTableCodeResult SaveStructFile(GenerateTableCodeResult result)
        {
            result.FileName ??= result.ClassName + ".cs";
            File.WriteAllText(result.FileName, result.Code);
            return result;
        }
    }
}
