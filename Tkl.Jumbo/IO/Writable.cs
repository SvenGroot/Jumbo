// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for writable types that provides a default implementation
    /// of <see cref="IWritable"/> based on reflection and code generation.
    /// </summary>
    /// <typeparam name="T">The type of the writable type; this should be the type inheriting from <see cref="Writable{T}"/>.</typeparam>
    public abstract class Writable<T> : IWritable
        where T : Writable<T>
    {
        private static Action<T, BinaryWriter> _writeMethod = WritableUtility.CreateSerializer<T>();
        private static Action<T, BinaryReader> _readMethod = WritableUtility.CreateDeserializer<T>();

        #region IWritable Members

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public virtual void Write(BinaryWriter writer)
        {
            _writeMethod((T)this, writer);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            _readMethod((T)this, reader);
        }

        #endregion
    }
}
