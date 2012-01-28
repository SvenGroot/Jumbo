// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Specifies the <paramref name="IRawComparer{T}"/> implementation for a type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class RawComparerAttribute : Attribute
    {
        private readonly string _rawComparerTypeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="RawComparerAttribute"/> class.
        /// </summary>
        /// <param name="RawComparerTypeName">The type name of the type implementing <see cref="IRawComparer{T}"/>.</param>
        public RawComparerAttribute(string RawComparerTypeName)
        {
            _rawComparerTypeName = RawComparerTypeName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RawComparerAttribute"/> class.
        /// </summary>
        /// <param name="rawComparerTypeName">The type that implements <see cref="IRawComparer{T}"/>.</param>
        public RawComparerAttribute(Type rawComparerTypeName)
        {
            if( rawComparerTypeName == null )
                throw new ArgumentNullException("rawComparerTypeName");
            _rawComparerTypeName = rawComparerTypeName.AssemblyQualifiedName;
        }

        /// <summary>
        /// Gets the name of the type that implements <see cref="IRawComparer{T}"/>.
        /// </summary>
        /// <value>
        /// The name of a type that implements <see cref="IRawComparer{T}"/>.
        /// </value>
        public string RawComparerTypeName
        {
            get { return _rawComparerTypeName; }
        }
    }
}
