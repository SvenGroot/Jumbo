﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Specifies the <see cref="IValueWriter{T}"/> to use to serialize a type.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This is for use with value types or immutable reference types. For mutable reference types it's recommended
    ///   that you implement <see cref="IWritable"/> instead.
    /// </para>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
    public sealed class ValueWriterAttribute : Attribute
    {
        private readonly string _valueWriterTypeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="ValueWriterAttribute"/> class.
        /// </summary>
        /// <param name="typeName">The type name of the type implementing <see cref="IValueWriter{T}"/>.</param>
        public ValueWriterAttribute(string typeName)
        {
            _valueWriterTypeName = typeName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ValueWriterAttribute"/> class.
        /// </summary>
        /// <param name="type">The type that implements <see cref="IValueWriter{T}"/>.</param>
        public ValueWriterAttribute(Type type)
        {
            _valueWriterTypeName = type.AssemblyQualifiedName;
        }

        /// <summary>
        /// Gets the name of the type that implements <see cref="IValueWriter{T}"/>.
        /// </summary>
        /// <value>
        /// The name of a type that implements <see cref="IValueWriter{T}"/>.
        /// </value>
        public string ValueWriterTypeName
        {
            get { return _valueWriterTypeName; }
        }
    }
}
