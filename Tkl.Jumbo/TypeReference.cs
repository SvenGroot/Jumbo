using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Represents a reference to a <see cref="Type"/> that will be serialized to XML using the type name.
    /// </summary>
    public struct TypeReference : IXmlSerializable
    {
        private string _typeName;
        private Type _type;

        /// <summary>
        /// Initializes a new instance of the <see cref="TypeReference"/> structure using the specified type.
        /// </summary>
        /// <param name="type">The type this instance should reference. May be <see langword="null"/>.</param>
        public TypeReference(Type type)
        {
            _type = type;
            _typeName = type == null ? null : type.AssemblyQualifiedName;
        }

        /// <summary>
        /// Gets or sets the type that this <see cref="TypeReference"/> references.
        /// </summary>
        public Type Type
        {
            get
            {
                if( _type == null && _typeName != null )
                    _type = Type.GetType(_typeName, true);
                return _type;
            }
            set
            {
                _type = value;
                if( value != null )
                    _typeName = _type.AssemblyQualifiedName;
            }
        }

        /// <summary>
        /// Gets or sets the name of the type that this <see cref="TypeReference"/> references.
        /// </summary>
        public string TypeName
        {
            get
            {
                return _typeName;
            }
            set
            {
                _typeName = value;
                _type = null;
            }
        }

        /// <summary>
        /// Converts this instance to a string representation.
        /// </summary>
        /// <returns>The name of the type that this <see cref="TypeReference"/> references, or an empty string if <see cref="TypeName"/> is <see langword="null"/>.</returns>
        public override string ToString()
        {
            return TypeName ?? string.Empty;
        }

        /// <summary>
        /// Implicitly converts a <see cref="Type"/> to a <see cref="TypeReference"/>.
        /// </summary>
        /// <param name="type">The type to reference.</param>
        /// <returns>An instance of <see cref="TypeReference"/> that references <paramref name="type"/>.</returns>
        public static implicit operator TypeReference(Type type)
        {
            return new TypeReference(type);
        }

        #region IXmlSerializable Members

        System.Xml.Schema.XmlSchema IXmlSerializable.GetSchema()
        {
            return null;
        }

        void IXmlSerializable.ReadXml(System.Xml.XmlReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            if( reader.IsEmptyElement )
                reader.ReadStartElement();
            else
            {
                TypeName = reader.ReadString();
                reader.ReadEndElement();
            }
        }

        void IXmlSerializable.WriteXml(System.Xml.XmlWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            writer.WriteString(TypeName);
        }

        #endregion
    }
}
