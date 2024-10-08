using System;
using System.IO;

namespace Cave.Data;

/// <summary>Provides an abstract base class for file databases containing multiple tables.</summary>
public abstract class FileDatabase : Database, IDisposable
{
    #region Protected Constructors

    /// <summary>Initializes a new instance of the <see cref="FileDatabase"/> class.</summary>
    /// <param name="storage">The storage engine.</param>
    /// <param name="directory">The directory the database can be found at.</param>
    protected FileDatabase(FileStorage storage, string directory)
        : base(storage, Path.GetFileName(directory))
    {
        Folder = directory ?? throw new ArgumentNullException(nameof(directory));
        Directory.CreateDirectory(Folder);
    }

    #endregion Protected Constructors

    #region Protected Methods

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing) => Close();

    #endregion Protected Methods

    #region Public Properties

    /// <summary>Gets the path (directory) the database can be found at.</summary>
    public string? Folder { get; private set; }

    /// <summary>Gets a value indicating whether the instance was already closed.</summary>
    public override bool IsClosed => Folder == null;

    #endregion Public Properties

    #region Public Methods

    /// <summary>Closes the instance and flushes all cached data.</summary>
    public override void Close() => Folder = null;

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Gets the name of the database.</summary>
    /// <returns>The name of the database.</returns>
    public override string ToString() => Name;

    #endregion Public Methods
}
