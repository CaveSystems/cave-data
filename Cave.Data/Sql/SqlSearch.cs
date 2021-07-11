using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using Cave.Collections.Generic;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a class used during custom searches to keep up with all parameters to be added during sql command generation.
    /// </summary>
    public sealed class SqlSearch
    {
        #region Private Fields

        readonly IndexedSet<string> FieldNameSet = new IndexedSet<string>();
        RowLayout Layout => Table.Layout;
        readonly List<SqlParam> ParameterList = new List<SqlParam>();
        SqlStorage Storage => Table.Database.Storage as SqlStorage;
        readonly string Text;
        readonly SqlTable Table;

        #endregion Private Fields

        #region Private Methods

        /// <summary>
        /// Adds a new parameter.
        /// </summary>
        /// <param name="databaseValue">The databaseValue of the parameter.</param>
        /// <returns>A new <see cref="SqlParam"/> instance.</returns>
        SqlParam AddParameter(object databaseValue)
        {
            var name = Storage.SupportsNamedParameters ? $"{Storage.ParameterPrefix}{Parameters.Count + 1}" : Storage.ParameterPrefix;
            var parameter = new SqlParam(name, databaseValue);
            ParameterList.Add(parameter);
            return parameter;
        }

        void Flatten(StringBuilder sb, Search search)
        {
            search.LoadLayout(Layout, Table.GetFieldNameComparison());
            switch (search.Mode)
            {
                case SearchMode.None:
                sb.Append("1=1");
                return;

                case SearchMode.In:
                {
                    FieldNameSet.Include(search.FieldName);
                    var fieldName = Storage.EscapeFieldName(search.FieldProperties);
                    sb.Append(fieldName);
                    sb.Append(' ');
                    if (search.Inverted)
                    {
                        sb.Append("NOT ");
                    }

                    sb.Append("IN (");
                    var i = 0;
                    foreach (var value in (Set<object>)search.FieldValue)
                    {
                        if (i++ > 0)
                        {
                            sb.Append(',');
                        }

                        var dbValue = Storage.GetDatabaseValue(search.FieldProperties, value);
                        var parameter = AddParameter(dbValue);
                        sb.Append(parameter.Name);
                    }

                    sb.Append(')');
                    return;
                }
                case SearchMode.Equals:
                {
                    FieldNameSet.Include(search.FieldName);
                    var fieldName = Storage.EscapeFieldName(search.FieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (search.FieldValue == null)
                    {
                        sb.Append($"{fieldName} IS {(search.Inverted ? "NOT " : string.Empty)}NULL");
                    }
                    else
                    {
                        // no add parameter and return querytext
                        var dbValue = Storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                        var parameter = AddParameter(dbValue);
                        sb.Append($"{fieldName} {(search.Inverted ? "<>" : "=")} {parameter.Name}");
                    }

                    break;
                }
                case SearchMode.Like:
                {
                    FieldNameSet.Include(search.FieldName);
                    var fieldName = Storage.EscapeFieldName(search.FieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (search.FieldValue == null)
                    {
                        sb.Append($"{fieldName} IS {(search.Inverted ? "NOT " : string.Empty)}NULL");
                    }
                    else
                    {
                        // no add parameter and return querytext
                        var dbValue = Storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                        var parameter = AddParameter(dbValue);
                        sb.Append($"{fieldName} {(search.Inverted ? "NOT " : string.Empty)}LIKE {parameter.Name}");
                    }

                    break;
                }
                case SearchMode.Greater:
                {
                    FieldNameSet.Include(search.FieldName);
                    var fieldName = Storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = Storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{fieldName}<={parameter.Name}" : $"{fieldName}>{parameter.Name}");
                    break;
                }
                case SearchMode.GreaterOrEqual:
                {
                    FieldNameSet.Include(search.FieldName);
                    var fieldName = Storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = Storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{fieldName}<{parameter.Name}" : $"{fieldName}>={parameter.Name}");
                    break;
                }
                case SearchMode.Smaller:
                {
                    FieldNameSet.Include(search.FieldName);
                    var name = Storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = Storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{name}>={parameter.Name}" : $"{name}<{parameter.Name}");
                    break;
                }
                case SearchMode.SmallerOrEqual:
                {
                    FieldNameSet.Include(search.FieldName);
                    var name = Storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = Storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{name}>{parameter.Name}" : $"{name}<={parameter.Name}");
                    break;
                }
                case SearchMode.And:
                {
                    if (search.Inverted)
                    {
                        sb.Append("NOT ");
                    }

                    sb.Append('(');
                    Flatten(sb, search.SearchA);
                    sb.Append(" AND ");
                    Flatten(sb, search.SearchB);
                    sb.Append(')');
                    break;
                }
                case SearchMode.Or:
                {
                    if (search.Inverted)
                    {
                        sb.Append("NOT ");
                    }

                    sb.Append('(');
                    Flatten(sb, search.SearchA);
                    sb.Append(" OR ");
                    Flatten(sb, search.SearchB);
                    sb.Append(')');
                    break;
                }
                default: throw new NotImplementedException($"Mode {search.Mode} not implemented!");
            }
        }

        #endregion Private Methods

        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlSearch"/> class.
        /// </summary>
        /// <param name="table">Table the search is performed on.</param>
        /// <param name="search">Search to perform.</param>
        internal SqlSearch(SqlTable table, Search search)
        {
            if (search == null)
            {
                throw new ArgumentNullException(nameof(search));
            }

            this.Table = table;
            Parameters = new ReadOnlyCollection<SqlParam>(ParameterList);
            FieldNames = new ReadOnlyCollection<string>(FieldNameSet);
            var sb = new StringBuilder();
            Flatten(sb, search);
            Text = sb.ToString();
        }

        #endregion Public Constructors

        #region Public Properties

        /// <summary>
        /// Gets the field names.
        /// </summary>
        public IList<string> FieldNames { get; }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public IList<SqlParam> Parameters { get; }

        #endregion Public Properties

        #region Public Methods

        /// <summary>
        /// Checks whether all fields used at options are present and adds them if not.
        /// </summary>
        /// <param name="option">Options to check.</param>
        public void CheckFieldsPresent(ResultOption option)
        {
            if (option == null)
            {
                throw new ArgumentNullException(nameof(option));
            }

            foreach (var fieldName in option.FieldNames)
            {
                if (!FieldNameSet.Contains(fieldName))
                {
                    FieldNameSet.Add(fieldName);
                }
            }
        }

        /// <summary>
        /// Gets the query text as string.
        /// </summary>
        /// <returns>A the database specific search string.</returns>
        public override string ToString() => Text;

        #endregion Public Methods
    }
}
