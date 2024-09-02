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
}
