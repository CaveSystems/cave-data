using System;
using System.IO;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides an interface to subversioned files and directories (reads .svn/entries).
    /// </summary>
    public static class Subversion
    {
        #region Private Methods

        /// <summary>
        /// Gets the root .svn path of the repository.
        /// </summary>
        /// <param name="path">The current path.</param>
        /// <returns>The root path.</returns>
        static string GetRootPath(string path)
        {
            path = Path.GetFullPath(path);
            while (true)
            {
                var result = Path.Combine(path, ".svn");
                if (Directory.Exists(result))
                {
                    return result;
                }

                path = Path.GetDirectoryName(path);
            }

            throw new DirectoryNotFoundException("Could not find .svn directory!");
        }

        static string[] ReadEntries(string path)
        {
            return GetVersion(path) switch
            {
                8 or 9 or 10 => File.ReadAllLines(Path.Combine(GetRootPath(path), "entries")),
                _ => throw new NotSupportedException(),
            };
        }

        #endregion Private Methods

        #region Public Methods

        /// <summary>
        /// Gets the revision of a specified directory.
        /// </summary>
        /// <param name="path">The path to check.</param>
        /// <returns>The revision number.</returns>
        public static int GetRevision(string path)
        {
            var svnRoot = GetRootPath(path);
            if (GetVersion(path) > 10)
            {
                using var storage = new SQLiteStorage(@"file:///" + svnRoot);
                var revision = (long)storage.QueryValue(database: "wc", table: "nodes", cmd: "SELECT MAX(revision) FROM " + storage.FQTN("wc", "nodes"));
                return (int)revision;
            }

            if (!Directory.Exists(svnRoot))
            {
                throw new DirectoryNotFoundException();
            }

            try
            {
                var svnEntries = ReadEntries(svnRoot);
                return int.Parse(svnEntries[3], null);
            }
            catch
            {
                throw new InvalidDataException("Cannot determine svn revision!");
            }
        }

        /// <summary>
        /// Gets the subversion version this repository was written by.
        /// </summary>
        /// <param name="path">Path to check.</param>
        /// <returns>The version code.</returns>
        public static int GetVersion(string path)
        {
            var svnRoot = GetRootPath(path);
            var entriesFile = Path.Combine(svnRoot, "entries");
            if (File.Exists(entriesFile))
            {
                var svnEntries = File.ReadAllLines(entriesFile);
                return svnEntries[0] switch
                {
                    "8" or "9" or "10" or "12" => int.Parse(svnEntries[0], null),
                    _ => throw new InvalidDataException($"Unknown svn version {svnEntries[0]}!"),
                };
            }

            var databaseFile = Path.Combine(svnRoot, "wc.db");
            if (File.Exists(databaseFile))
            {
                return 100;
            }

            throw new DirectoryNotFoundException("Could not find .svn directory!");
        }

        #endregion Public Methods
    }
}
