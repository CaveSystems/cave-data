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

using Cave.IO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides an index for <see cref="DatStorage"/>
    /// </summary>
    sealed class DatIndex : IEnumerable<DatEntry>, IDisposable
    {
        class DatEntryEnumerator : IEnumerator<DatEntry>
        {
            DataReader reader;
            long lastPosition;

            public DatEntryEnumerator(DataReader reader)
            {
                this.reader = reader;
                lastPosition = 4;
            }

            public DatEntry Current { get; private set; }

            object IEnumerator.Current { get { return Current; } }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (lastPosition >= reader.BaseStream.Length)
                {
                    return false;
                }

                reader.BaseStream.Position = lastPosition;
                Current = new DatEntry(reader);
                lastPosition = reader.BaseStream.Position;
                return true;
            }

            public void Reset()
            {
                lastPosition = 4;
            }
        }

        #region private implementation
        DataWriter Writer;
        DataReader Reader;
        Stream Stream;
        long LastUsedID;
        //long StartPosition;
        #endregion

        #region constructor
        /// <summary>
        /// Creates a new empty <see cref="DatIndex"/>
        /// </summary>
        public DatIndex(string fileName)
        {
            Stream = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            Writer = new DataWriter(Stream);
            Reader = new DataReader(Stream);
            if (Stream.Length < 4)
            {
                Writer.Write("IDX ");
            }
            else
            {
                if (Reader.ReadString(4) != "IDX ")
                {
                    throw new InvalidDataException();
                }
            }

            foreach (DatEntry entry in this)
            {
                if (entry.ID > LastUsedID)
                {
                    LastUsedID = entry.ID;
                }

                if (entry.ID <= 0)
                {
                    FreeItemCount++;
                }

                Count++;
            }
        }
        #endregion

        #region public implementation

        /// <summary>
        /// Obtains the next free (unused) ID
        /// </summary>
        /// <returns>Returns an unused ID</returns>
        public long GetNextFreeID()
        {
            return Math.Max(1, LastUsedID + 1);
        }

        /// <summary>
        /// Obtains the next used ID
        /// </summary>
        /// <param name="id">The (previous) ID to start search at</param>
        /// <returns>Returns an ID or -1</returns>
        public long GetNextUsedID(long id)
        {
            long best = long.MaxValue;
            foreach(DatEntry e in this)
            {
                if (e.ID > id && e.ID < best)
                {
                    best = e.ID;
                }
            }
            if (best == long.MaxValue)
            {
                return -1;
            }

            return best;
        }

        void SaveAtCurrentPosition(DatEntry entry)
        {
            entry.Save(Writer);
        }

        /// <summary>
        /// Adds a new <see cref="DatEntry"/>
        /// </summary>
        /// <param name="entry">The <see cref="DatEntry"/> to add to the index</param>
        public void Save(DatEntry entry)
        {
            if (entry.ID <= 0)
            {
                throw new ArgumentException(string.Format("Invalid ID!"));
            }

            if (entry.ID > LastUsedID)
            {
                LastUsedID = entry.ID;
            }

            //find 
            if (FreeItemCount > 0)
            {
                foreach (DatEntry e in this)
                {
                    if (e.ID <= 0)
                    {
                        FreeItemCount--;
                        Stream.Position -= e.Length;
                        SaveAtCurrentPosition(entry);
                        return;
                    }
                }
            }
            //append at end
            Stream.Position = Stream.Length;
            SaveAtCurrentPosition(entry);
            Count++;
        }        

        /// <summary>
        /// Obtains the number of IDs (entries) currently present at the index
        /// </summary>
        public long Count { get; private set; }

        /// <summary>
        /// Obtains the number of free (entries) currently present at the index
        /// </summary>
        public long FreeItemCount { get; private set; }

        /// <summary>
        /// Releases a <see cref="DatEntry"/> (removes an entry from the index)
        /// </summary>
        /// <param name="source">The source <see cref="DatEntry"/> to remove</param>
        public void Free(DatEntry source)
        {
            DatEntry entry = new DatEntry(0, source.BucketPosition, source.BucketLength);
            lock (Stream)
            {
                foreach (DatEntry e in this)
                {
                    if (e.ID == source.ID)
                    {
                        Stream.Position -= e.Length;
                        SaveAtCurrentPosition(entry);
                        FreeItemCount++;
                        return;
                    }
                }
                throw new ArgumentException();
            }
        }

        /// <summary>
        /// Obtains a free entry from the index for reuse
        /// </summary>
        /// <param name="id">The ID of the dataset to be written</param>
        /// <param name="count">The length the entry should have</param>
        /// <returns>Returns a free <see cref="DatEntry"/> or null</returns>
        public DatEntry GetFree(long id, int count)
        {
            if (FreeItemCount > 0)
            {
                foreach (DatEntry entry in this)
                {
                    if (entry.ID > 0)
                    {
                        continue;
                    }

                    if (entry.BucketLength >= count)
                    {
                        Stream.Position -= entry.Length;
                        DatEntry result = new DatEntry(id, entry.BucketPosition, entry.BucketLength);
                        SaveAtCurrentPosition(result);
                        FreeItemCount--;
                        return result;
                    }
                }
            }
            return default(DatEntry);
        }

        /// <summary>Checks whether the specified ID has an <see cref="DatEntry" /> at the index</summary>
        /// <param name="id">The ID to lookup</param>
        /// <param name="entry">The entry.</param>
        /// <returns>Returns true if the ID has an entry at the index</returns>
        public bool TryGet(long id, out DatEntry entry)
        {
            foreach (DatEntry e in this)
            {
                if (e.ID == id)
                {
                    entry = e;
                    return true;
                }
            }
            entry = default(DatEntry);
            return false;
        }

        public IEnumerable<long> IDs { get { return this.Select(e => e.ID); } }
        #endregion

        /// <summary>
        /// DatIndex[IDs:0,Free:0]
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("DatIndex[IDs:{0},Free:{1}]", Count, FreeItemCount);
        }

        public IEnumerator<DatEntry> GetEnumerator()
        {
            return new DatEntryEnumerator(Reader);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new DatEntryEnumerator(Reader);
        }

        public void Dispose()
        {
            Stream?.Close();
            Stream = null;
        }
    }
}