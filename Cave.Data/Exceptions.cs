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

using System;
using System.Runtime.Serialization;

namespace Cave.Data
{
    /// <summary>
    /// The table layout is already fixed and can no longer be changed !
    /// </summary>
    [Serializable]
    public class TableLayoutFixedException : Exception
    {
        /// <summary>
        /// The table layout is already fixed and can no longer be changed !
        /// </summary>
        public TableLayoutFixedException() : base(string.Format("The table layout is already fixed and can no longer be changed!")) { }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="msg">The message.</param>
        public TableLayoutFixedException(string msg) : base(msg) { }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="msg">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public TableLayoutFixedException(string msg, Exception innerException) : base(msg, innerException) { }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="info">The information.</param>
        /// <param name="context">The context.</param>
        protected TableLayoutFixedException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// The dataset ID {0} is already present!
    /// </summary>
    [Serializable]
    public class DataSetAlreadyPresentException : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        public DataSetAlreadyPresentException() : base("The dataset is already present!") { }

        /// <summary>
        /// The dataset ID {0} is already present at the table!
        /// </summary>
        public DataSetAlreadyPresentException(long id) : base(string.Format("The dataset ID {0} is already present!", id)) { }

        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        /// <param name="msg">The message.</param>
        public DataSetAlreadyPresentException(string msg) : base(msg) { }

        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        /// <param name="msg">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public DataSetAlreadyPresentException(string msg, Exception innerException) : base(msg, innerException) { }

        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        /// <param name="info">The information.</param>
        /// <param name="context">The context.</param>
        protected DataSetAlreadyPresentException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}