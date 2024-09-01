using System.Reflection;

namespace Cave.Data;

public record BaseRecord
{
#if NET20_OR_GREATER && !NET40_OR_GREATER
    FieldInfo[] fields = null;

    /// <inheritdoc/>
    public virtual bool Equals(BaseRecord other)
    {
        if (ReferenceEquals(this, other)) return true;
        if (other is null) return false;
        if (EqualityContract != other.EqualityContract) return false;
        fields ??= EqualityContract.GetFields(BindingFlags.NonPublic | BindingFlags.Public);
        foreach (var field in fields)
        {
            if (!Equals(field.GetValue(this), field.GetValue(other))) return false;
        }
        return true;
    }
#endif
}
