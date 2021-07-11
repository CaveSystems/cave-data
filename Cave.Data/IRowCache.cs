namespace Cave.Data
{
    /// <summary>Provides an interface for row caches.</summary>
    public interface IRowCache
    {
        #region Properties

        /// <summary>Gets the number of cache hits.</summary>
        long HitCount { get; }

        /// <summary>Gets the number of cache misses.</summary>
        long MissCount { get; }

        /// <summary>Gets the number of row not found at table results.</summary>
        long NotFoundCount { get; }

        #endregion

        #region Members

        /// <summary>Clears the cache and all counters.</summary>
        void Clear();

        #endregion
    }
}
