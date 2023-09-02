using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Cave.Data
{
    /// <summary>
    /// Provides resultset options for search, sort and grouping functions.
    /// </summary>
    public sealed class ResultOption : IEnumerable<ResultOption>
    {
        #region Private Fields

        readonly IEnumerable<ResultOption> list;

        #endregion Private Fields

        #region Private Constructors

        ResultOption(IEnumerable<ResultOption> list)
        {
            var copy = list.First();
            Mode = copy.Mode;
            Parameter = copy.Parameter;
            this.list = list;
        }

        ResultOption(ResultOptionMode mode, string parameter)
        {
            Mode = mode;
            Parameter = parameter;
        }

        #endregion Private Constructors

        #region Public Properties

        /// <summary>
        /// Gets no option.
        /// </summary>
        public static ResultOption None { get; } = new ResultOption(ResultOptionMode.None, null);

        /// <summary>
        /// Gets the field names.
        /// </summary>
        /// <value>The field names.</value>
        public IList<string> FieldNames
        {
            get
            {
                var result = new List<string>();
                foreach (var option in this)
                {
                    switch (option.Mode)
                    {
                        case ResultOptionMode.SortAsc:
                        case ResultOptionMode.SortDesc:
                        case ResultOptionMode.Group:
                            result.Add(option.Parameter);
                            break;
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// Gets the mode.
        /// </summary>
        /// <value>The mode.</value>
        public ResultOptionMode Mode { get; }

        /// <summary>
        /// Gets the parameter name.
        /// </summary>
        /// <value>The parameter name.</value>
        public string Parameter { get; }

        #endregion Public Properties

        #region Public Methods

        /// <summary>
        /// Group the fields with the specified fieldname.
        /// </summary>
        /// <param name="field">Field to group.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption Group(string field) => new(ResultOptionMode.Group, field);

        /// <summary>
        /// Limit the number of result sets.
        /// </summary>
        /// <param name="resultCount">Number of results to fetch.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption Limit(int resultCount) =>
            resultCount < 0
                ? throw new ArgumentOutOfRangeException(nameof(resultCount))
                : new ResultOption(ResultOptionMode.Limit, resultCount.ToString(CultureInfo.InvariantCulture));

        /// <summary>
        /// Set start offset of result sets.
        /// </summary>
        /// <param name="offset">Offset at the result list.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption Offset(int offset) =>
            offset < 0
                ? throw new ArgumentOutOfRangeException(nameof(offset))
                : new ResultOption(ResultOptionMode.Offset, offset.ToString(CultureInfo.InvariantCulture));

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The first item.</param>
        /// <param name="right">The second item.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator !=(ResultOption left, ResultOption right) => !(left == right);

        /// <summary>
        /// Combines two <see cref="ResultOption"/> s with AND.
        /// </summary>
        /// <param name="left">The first item.</param>
        /// <param name="right">The second item.</param>
        /// <returns>The result of the operator.</returns>
        public static ResultOption operator +(ResultOption left, ResultOption right) => new(left.Concat(right));

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="left">The first item.</param>
        /// <param name="right">The second item.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator ==(ResultOption left, ResultOption right)
        {
            if (right is null)
            {
                return left is null;
            }

            return left is object && (left.Mode == right.Mode) && (left.Parameter == right.Parameter);
        }

        /// <summary>
        /// Sort ascending by the specified fieldname.
        /// </summary>
        /// <param name="field">The field to sort.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption SortAscending(string field) => new(ResultOptionMode.SortAsc, field);

        /// <summary>
        /// Sort ascending by the specified fieldname.
        /// </summary>
        /// <param name="fields">The fields to sort.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption SortAscending(params string[] fields)
        {
            var result = None;
            if (fields != null)
            {
                foreach (var field in fields)
                {
                    result += SortAscending(field);
                }
            }

            return result;
        }

        /// <summary>
        /// Sort descending by the specified fieldname.
        /// </summary>
        /// <param name="field">The field to sort.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption SortDescending(string field) => new(ResultOptionMode.SortDesc, field);

        /// <summary>
        /// Sort descending by the specified fieldname.
        /// </summary>
        /// <param name="fields">The fields to sort.</param>
        /// <returns>A new <see cref="ResultOption"/> instance.</returns>
        public static ResultOption SortDescending(params string[] fields)
        {
            var result = None;
            if (fields != null)
            {
                foreach (var field in fields)
                {
                    result += SortDescending(field);
                }
            }

            return result;
        }

        /// <summary>
        /// Determines whether [contains] [the specified modes].
        /// </summary>
        /// <param name="modes">The modes.</param>
        /// <returns><c>true</c> if [contains] [the specified modes]; otherwise, <c>false</c>.</returns>
        public bool Contains(params ResultOptionMode[] modes)
        {
            foreach (var option in this)
            {
                if (Array.IndexOf(modes, option.Mode) > -1)
                {
                    return true;
                }
            }

            return false;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            var o = obj as ResultOption;
            return (o != null) && (o.Mode == Mode) && (o.Parameter == Parameter);
        }

        /// <summary>
        /// Returns an array with all options with the specified modes.
        /// </summary>
        /// <param name="modes">The modes.</param>
        /// <returns>Returns an array with all matching options.</returns>
        public ResultOption[] Filter(params ResultOptionMode[] modes)
        {
            var results = new List<ResultOption>();
            foreach (var option in this)
            {
                if (Array.IndexOf(modes, option.Mode) > -1)
                {
                    results.Add(option);
                }
            }

            return results.ToArray();
        }

        /// <inheritdoc/>
        public override int GetHashCode() => ToString().GetHashCode();

        /// <inheritdoc/>
        public override string ToString()
        {
            var result = new StringBuilder();
            foreach (var option in this)
            {
                if (result.Length > 0)
                {
                    result.Append(", ");
                }

                result.Append(option.Mode.ToString());
                if (Parameter != null)
                {
                    result.Append('[');
                    result.Append(option.Parameter);
                    result.Append(']');
                }
            }

            return result.ToString();
        }

        /// <inheritdoc/>
        public IEnumerator<ResultOption> GetEnumerator() => (list ?? new[] { this }).Where(i => i.Mode != ResultOptionMode.None).GetEnumerator();

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => (list ?? new[] { this }).Where(i => i.Mode != ResultOptionMode.None).GetEnumerator();

        #endregion Public Methods
    }
}
