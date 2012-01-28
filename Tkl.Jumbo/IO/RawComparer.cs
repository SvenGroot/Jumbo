// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides access to the <see cref="IRawComparer{T}"/> instance for a particular <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The type of the objects being compared.</typeparam>
    public static class RawComparer<T>
    {
        private static readonly IRawComparer<T> _instance = GetComparer();

        /// <summary>
        /// Gets the <see cref="IRawComparer{T}"/> instance, or <see langword="null"/> if the <paramref name="T"/> doesn't have
        /// a raw comparer.
        /// </summary>
        public static IRawComparer<T> Instance
        {
            get { return _instance; }
        }

        private static IRawComparer<T> GetComparer()
        {
            Type type = typeof(T);
            RawComparerAttribute attribute = (RawComparerAttribute)Attribute.GetCustomAttribute(type, typeof(RawComparerAttribute));
            if( attribute != null && !string.IsNullOrEmpty(attribute.RawComparerTypeName) )
            {
                Type comparerType = Type.GetType(attribute.RawComparerTypeName);
                return (IRawComparer<T>)Activator.CreateInstance(comparerType);
            }

            return (IRawComparer<T>)DefaultRawComparer.GetComparer(type);
        }
    }
}
