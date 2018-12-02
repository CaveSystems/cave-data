#region CopyRight 2018
/*
    Copyright (c) 2005-2018 Andreas Rohleder (andreas@rohleder.cc)
    All rights reserved
*/
#endregion
#region License LGPL-3
/*
    This program/library/sourcecode is free software; you can redistribute it
    and/or modify it under the terms of the GNU Lesser General Public License
    version 3 as published by the Free Software Foundation subsequent called
    the License.

    You may not use this program/library/sourcecode except in compliance
    with the License. The License is included in the LICENSE file
    found at the installation directory or the distribution package.

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#endregion License
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion Authors & Contributors

using Cave.Text;
using System;
using System.Diagnostics;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides access to database storage engines
    /// </summary>
    public abstract class Storage : IStorage
    {
        ConnectionString m_ConnectionString;
        bool m_Closed;

		/// <summary>Creates a new storage instance with the specified <see cref="ConnectionString" /></summary>
		/// <param name="connectionString">ConnectionString of the storage</param>
		/// <param name="options">The options.</param>
		protected Storage(ConnectionString connectionString, DbConnectionOptions options)
        {
            m_ConnectionString = connectionString;
			LogVerboseMessages = options.HasFlag(DbConnectionOptions.VerboseLogging);
			if (LogVerboseMessages)
            {
                Trace.TraceInformation("Verbose logging <green>enabled!");
            }
        }

        #region abstract IStorage Member

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public abstract bool HasDatabase(string database);

        /// <summary>
        /// Obtains all available database names
        /// </summary>
        public abstract string[] DatabaseNames { get; }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public abstract IDatabase GetDatabase(string database);

        /// <summary>
        /// Adds a new database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public abstract IDatabase CreateDatabase(string database);

        /// <summary>
        /// Removes the specified database
        /// </summary>
        /// <param name="database">The name of the database</param>
        public abstract void DeleteDatabase(string database);

        #endregion

        #region implemented IStorage Member

        /// <summary>
        /// Allow delayed inserts, updates and deletes
        /// </summary>
        public bool UseDelayedWrites { get; set; }

        /// <summary>
        /// Obtains FieldProperties for the Database based on requested FieldProperties
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public virtual FieldProperties GetDatabaseFieldProperties(FieldProperties field)
        {
            return field;
        }

        /// <summary>
        /// Gets/sets the <see cref="ConnectionString"/> used to connect to the database server
        /// </summary>
        public virtual ConnectionString ConnectionString
        {
            get { return m_ConnectionString; }
        }

        /// <summary>
        /// Obtains wether the storage was already closed or not
        /// </summary>
        public virtual bool Closed { get { return m_Closed; } }

        /// <summary>
        /// closes the connection to the storage engine
        /// </summary>
        public virtual void Close()
        {
            m_Closed = true;
        }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <param name="createIfNotExists">Create the database if its not already present</param>
        /// <returns></returns>
        public virtual IDatabase GetDatabase(string database, bool createIfNotExists)
        {
            if (HasDatabase(database))
            {
                return GetDatabase(database);
            }

            if (createIfNotExists)
            {
                return CreateDatabase(database);
            }

            throw new ArgumentException(string.Format("The requested database '{0}' was not found!", database));
        }

        /// <summary>
        /// Checks two layouts for equality using the database field type conversion and throws an error if the layouts do not match
        /// </summary>
        /// <param name="expected">The expected layout</param>
        /// <param name="current">The layout to check</param>
        public virtual void CheckLayout(RowLayout expected, RowLayout current)
        {
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
                throw new InvalidDataException(string.Format("Fieldcount of table {0} differs (found {1} expected {2})!", current.Name, current.FieldCount, expected.FieldCount));
            }
            for (int i = 0; i < expected.FieldCount; i++)
            {
                var expectedField = GetDatabaseFieldProperties(expected.GetProperties(i));
                var currentField = GetDatabaseFieldProperties(current.GetProperties(i));
                if (!expectedField.Equals(currentField))
                {
                    throw new InvalidDataException(string.Format("Fieldproperties of table {0} differ! (found {1} expected {2})!", current.Name, currentField, expectedField));
                }
            }
        }

        #endregion

        #region precision members
        /// <summary>
        /// Obtains the maximum <see cref="float"/> precision at the value of 1.0f of this storage engine
        /// </summary>
        public virtual float FloatPrecision { get { return 0; } }

        /// <summary>
        /// Obtains the maximum <see cref="double"/> precision at the value of 1.0d of this storage engine
        /// </summary>
        public virtual double DoublePrecision { get { return 0; } }

        /// <summary>
        /// Obtains the maximum <see cref="DateTime"/> value precision of this storage engine
        /// </summary>
        public virtual TimeSpan DateTimePrecision { get { return TimeSpan.FromMilliseconds(0); } }

        /// <summary>
        /// Obtains the maximum <see cref="TimeSpan"/> value precision of this storage engine
        /// </summary>
        public virtual TimeSpan TimeSpanPrecision { get { return new TimeSpan(0); } }

        /// <summary>
        /// Obtains the maximum <see cref="decimal"/> value precision of this storage engine
        /// </summary>
        public virtual decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
            {
                return 0;
            }

            double l_PreDecimal = Math.Truncate(count);
            int l_Decimal = (int)Math.Round((count - l_PreDecimal) * 100);
            decimal result = 1;
            while (l_Decimal-- > 0)
            {
                result *= 0.1m;
            }
            return result;
        }
        #endregion

        /// <summary>Gets or sets a value indicating whether [log verbose messages].</summary>
        /// <value><c>true</c> if [log verbose messages]; otherwise, <c>false</c>.</value>
        public bool LogVerboseMessages { get; set; } 

        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public string LogSourceName { get { return m_ConnectionString.ToString(ConnectionStringPart.NoCredentials); } }

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public abstract bool SupportsNativeTransactions { get; }
    }
}