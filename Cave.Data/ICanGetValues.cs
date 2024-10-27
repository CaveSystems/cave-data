namespace Cave.Data;

/// <summary>Provides an interface for structures able to return an object array of all fields content.</summary>
public interface ICanGetValues
{
    #region Public Methods

    /// <summary>Gets an array with the content of all fields of the structure</summary>
    /// <returns></returns>
    object?[] GetValues();

    #endregion Public Methods
}
