using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a sql command builder.
    /// </summary>
    public sealed class SqlCommandBuilder
    {
        #region Private Fields

        readonly List<SqlParam> ParameterList = new();

        readonly SqlStorage Storage;

        readonly StringBuilder StringBuilder = new();

        #endregion Private Fields

        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlCommandBuilder"/> class.
        /// </summary>
        /// <param name="storage">The storage engine.</param>
        public SqlCommandBuilder(SqlStorage storage)
        {
            this.Storage = storage ?? throw new ArgumentNullException(nameof(storage));
            Parameters = new ReadOnlyCollection<SqlParam>(ParameterList);
        }

        #endregion Public Constructors

        #region Public Properties

        /// <summary>
        /// Gets the parameter count.
        /// </summary>
        public int ParameterCount => ParameterList.Count;

        /// <summary>
        /// Gets all parameters present.
        /// </summary>
        public IList<SqlParam> Parameters { get; }

        /// <summary>
        /// Gets the full command text.
        /// </summary>
        public string Text => StringBuilder.ToString();

        /// <summary>
        /// Gets the length of the command text.
        /// </summary>
        public int Length => StringBuilder.Length;

        #endregion Public Properties

        #region Public Methods

        /// <summary>
        /// Converts to a <see cref="SqlCmd"/> instance.
        /// </summary>
        /// <param name="builder">The builder to convert.</param>
        public static implicit operator SqlCmd(SqlCommandBuilder builder) => builder == null ? null : new SqlCmd(builder.ToString(), builder.Parameters);

        /// <summary>
        /// Appends a command text.
        /// </summary>
        /// <param name="text">Text to add.</param>
        public void Append(string text) => this.StringBuilder.Append(text);

        /// <summary>
        /// Appends a command text.
        /// </summary>
        /// <param name="text">Text to add.</param>
        public void AppendLine(string text) => this.StringBuilder.AppendLine(text);

        /// <summary>
        /// Appends a parameter to the command text and parameter list.
        /// </summary>
        /// <param name="vdatabaseValuelue">The value at the database.</param>
        public void CreateAndAddParameter(object vdatabaseValuelue) => StringBuilder.Append(CreateParameter(vdatabaseValuelue).Name);

        /// <summary>
        /// Appends a parameter to the parameter list.
        /// </summary>
        /// <param name="databaseValue">The value at the database.</param>
        /// <returns>A new parameter instance.</returns>
        public SqlParam CreateParameter(object databaseValue)
        {
            var name = Storage.ParameterPrefix;
            if (Storage.SupportsNamedParameters)
            {
                name += ParameterList.Count;
            }

            var parameter = new SqlParam(name, databaseValue);
            ParameterList.Add(parameter);
            return parameter;
        }

        /// <summary>
        /// Gets the full command text.
        /// </summary>
        /// <returns>Command text.</returns>
        public override string ToString() => StringBuilder.ToString();

        #endregion Public Methods
    }
}
