using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides access to <see cref="IValueWriter{T}"/> implementations for various basic framework types.
    /// </summary>
    public static class DefaultValueWriter
    {
        #region Nested types

        private class Int32Writer : IValueWriter<int>
        {
            public void Write(int value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public int Read(System.IO.BinaryReader reader)
            {
                return reader.ReadInt32();
            }
        }

        #endregion

        private static IValueWriter<int> _int32Writer;

        /// <summary>
        /// Gets a writer for the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static object GetWriter(Type type)
        {
            if( type == null )
                throw new ArgumentNullException("type");

            object result;
            // We cache writer instances in fields; those are assigned without synchronization, because no one cares if there's more than one instance.

            if( type == typeof(int) )
            {
                result = _int32Writer;
                if( result == null )
                {
                    result = new Int32Writer();
                    _int32Writer = (IValueWriter<int>)result;
                }
            }
            else
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Could not find the writer for type {0}.", type));

            return result;
        }
    }
}
