﻿namespace Cave.Data.Sql;

/// <summary>Provides a named sql parameter.</summary>
public sealed class SqlParam
{
    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="SqlParam"/> class.</summary>
    /// <param name="parameterName">Name of the parameter.</param>
    /// <param name="databaseValue">Value at the database.</param>
    public SqlParam(string parameterName, object? databaseValue)
    {
        Name = parameterName;
        Value = databaseValue;
    }

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the name of the parameter.</summary>
    public string Name { get; }

    /// <summary>Gets the database value.</summary>
    public object? Value { get; }

    #endregion Public Properties
}
