using System;
using System.Collections.Generic;
using System.IO;

namespace Cave.Data;

/// <summary>Provides an abstract base class for file storage containing multiple databases.</summary>
public abstract class FileStorage : Storage, IDisposable
{
    #region Protected Constructors

    /// <summary>
    /// Initializes a new instance of the <see cref="FileStorage"/> class.
    /// <para>Following formats are supported: <br/> file://server/relativepath <br/> file:absolutepath. <br/></para>
    /// </summary>
    /// <param name="connectionString">ConnectionString of the storage.</param>
    /// <param name="options">The options.</param>
    protected FileStorage(ConnectionString connectionString, ConnectionFlags options)
        : base(connectionString, options)
    {
        if (string.IsNullOrEmpty(connectionString.Server))
        {
            connectionString.Server = "localhost";
        }

        if (connectionString.Server is not "localhost" and not ".")
        {
            throw new NotSupportedException("Remote access via server setting is not supported atm.! (use localhost or .)");
        }

        if (string.IsNullOrEmpty(connectionString.Location) || !connectionString.Location!.Contains('/'))
        {
            connectionString.Location = $"./{connectionString.Location}";
        }

        Folder = Path.GetFullPath(Path.GetDirectoryName(connectionString.Location) ?? throw new InvalidOperationException("No path specified!"));
        if (!Directory.Exists(Folder))
        {
            try
            {
                Directory.CreateDirectory(Folder);
            }
            catch (Exception ex)
            {
                throw new DirectoryNotFoundException($"The directory '{connectionString.Location}' cannot be found or created!", ex);
            }
        }
    }

    #endregion Protected Constructors

    #region Protected Methods

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing) => Close();

    #endregion Protected Methods

    #region Public Properties

    /// <inheritdoc/>
    public override IList<string> DatabaseNames
    {
        get
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            var result = new List<string>();
            foreach (var directory in Directory.GetDirectories(Folder, "*", SearchOption.TopDirectoryOnly))
            {
                result.Add(Path.GetFileName(directory));
            }

            return result;
        }
    }

    /// <summary>Gets the base path used for the file storage.</summary>
    public string Folder { get; private set; }

    #endregion Public Properties

    #region Public Methods

    /// <inheritdoc/>
    public override IDatabase CreateDatabase(string databaseName)
    {
        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        try
        {
            Directory.CreateDirectory(Folder + databaseName);
            return GetDatabase(databaseName);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"The databaseName {databaseName} cannot be created!", ex);
        }
    }

    /// <inheritdoc/>
    public override void DeleteDatabase(string database)
    {
        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        Directory.Delete(Path.Combine(Folder, database), true);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc/>
    public override bool HasDatabase(string databaseName)
    {
        if (Closed)
        {
            throw new ObjectDisposedException(ToString());
        }

        return Directory.Exists(Path.Combine(Folder, databaseName));
    }

    /// <inheritdoc/>
    public override string ToString() => $"file://{Folder}";

    #endregion Public Methods
}
