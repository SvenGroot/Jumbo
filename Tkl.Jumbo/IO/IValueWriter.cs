using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Interface for binary serialization of built-in types with <see cref="RecordWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public interface IValueWriter<T>
    {
        /// <summary>
        /// Writes the specified value to the specified writer.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="writer">The writer.</param>
        void Write(T value, BinaryWriter writer);

        /// <summary>
        /// Reads a value from the specified reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        T Read(BinaryReader reader);
    }
}
