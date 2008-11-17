using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Interface for objects that can be serializes using the <see cref="BinaryWriter"/>
    /// and <see cref="BinaryReader"/> classes.
    /// </summary>
    public interface IWritable
    {
        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        void Write(BinaryWriter writer);

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        void Read(BinaryReader reader);
    }
}
