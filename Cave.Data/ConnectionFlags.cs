﻿using System;

namespace Cave.Data;

/// <summary>Database connection flags.</summary>
[Flags]
public enum ConnectionFlags
{
    /// <summary>No options</summary>
    None = 0,

    /// <summary>The allow unsafe connections without ssl/tls/encryption</summary>
    /// <remarks>All data and the credentials of the database user may be transmitted without any security!</remarks>
    AllowUnsafeConnections = 1 << 0,

    /// <summary>Allow to create the database if it does not exists</summary>
    AllowCreate = 1 << 1,

    /// <summary>Enable verbose logging</summary>
    VerboseLogging = 1 << 2
}
