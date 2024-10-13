using System;

namespace Cave.Data;


/// <summary>Options for <see cref="DatabaseInterfaceGenerator"/> and <see cref="TableInterfaceGenerator"/>.</summary>
public record InterfaceGeneratorOptions : BaseRecord
{
    /// <summary>
    /// If set to true <see cref="ITable{TKey, TStruct}"/> instances are used to access the database. If set to false <see cref="ITable{TStruct}"/> instances
    /// will be used.
    /// </summary>
    public bool DisableKnownIdentifiers { get; set; }

    /// <summary>Target directory for generated files</summary>
    public string OutputDirectory { get; set; } = ".";

    /// <summary>Generate GetHashCode() using only the identifier fields.</summary>
    public bool IdentifierHashCode { get; set; }

    /// <summary>Put the assembly version into the generated files at the [GeneratedCode] Attribute.</summary>
    public bool VersionHeader { get; set; }

    /// <summary>Provides a converter function for user defined field conversion. (E.g.: change type of specific field, modify settings or csharp name.)</summary>
    public Func<IFieldProperties, IFieldProperties>? FieldConverter { get; set; }

    /// <summary>Provides a function for user defined field visibility at generated structures.</summary>
    public Func<IFieldProperties, FieldVisibility>? FieldVisibility { get; set; }
}
