﻿using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Cave.Data.Sql;

/// <summary>Provides a sql command.</summary>
public sealed class SqlCmd
{
    #region Public Constructors

    /// <summary>Initializes a new instance of the <see cref="SqlCmd"/> class.</summary>
    /// <param name="text">Command text.</param>
    /// <param name="parameters">Command parameters.</param>
    public SqlCmd(string text, params SqlParam[] parameters)
    {
        Text = text;
        Parameters = new ReadOnlyCollection<SqlParam>(parameters);
    }

    /// <summary>Initializes a new instance of the <see cref="SqlCmd"/> class.</summary>
    /// <param name="text">Command text.</param>
    /// <param name="parameters">Command parameters.</param>
    public SqlCmd(string text, IList<SqlParam> parameters)
    {
        Text = text;
        if (parameters is not ReadOnlyCollection<SqlParam> ro)
        {
            ro = new ReadOnlyCollection<SqlParam>(parameters);
        }

        Parameters = ro;
    }

    #endregion Public Constructors

    #region Public Properties

    /// <summary>Gets the command parameters.</summary>
    public IList<SqlParam> Parameters { get; }

    /// <summary>Gets the command text.</summary>
    public string Text { get; }

    #endregion Public Properties

    #region Public Methods

    /// <summary>Implicit conversion from string (command text) to a <see cref="SqlCmd"/> instance.</summary>
    /// <param name="command">Command text.</param>
    public static implicit operator SqlCmd(string command) => new(command);

    /// <summary>Implicit conversion from <see cref="SqlCmd"/> instance to string (command text).</summary>
    /// <param name="command">Command instance.</param>
    public static implicit operator string(SqlCmd command) => command?.Text ?? string.Empty;

    #endregion Public Methods
}
