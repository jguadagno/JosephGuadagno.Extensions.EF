using System;

namespace JosephGuadagno.Extensions.EF
{
    /// <summary>
    ///     Provides validation functions
    /// </summary>
    internal static class Validation
    {
        public static void CheckArgumentNotNull<T>(T argumentValue, string argumentName)
            where T : class
        {
            if (null == argumentValue)
            {
                throw new ArgumentNullException(argumentName);
            }
        }
    }
}