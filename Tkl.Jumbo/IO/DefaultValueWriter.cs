// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides access to <see cref="IValueWriter{T}"/> implementations for various basic framework types.
    /// </summary>
    public static class DefaultValueWriter
    {
        #region Nested types

        private class SByteWriter : IValueWriter<SByte>
        {
            public void Write(SByte value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public SByte Read(BinaryReader reader)
            {
                return reader.ReadSByte();
            }
        }

        private class Int16Writer : IValueWriter<Int16>
        {
            public void Write(Int16 value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public Int16 Read(BinaryReader reader)
            {
                return reader.ReadInt16();
            }
        }

        private class Int32Writer : IValueWriter<Int32>
        {
            public void Write(int value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public int Read(BinaryReader reader)
            {
                return reader.ReadInt32();
            }
        }

        private class Int64Writer : IValueWriter<Int64>
        {
            public void Write(Int64 value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public Int64 Read(BinaryReader reader)
            {
                return reader.ReadInt64();
            }
        }

        private class ByteWriter : IValueWriter<Byte>
        {
            public void Write(Byte value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public Byte Read(BinaryReader reader)
            {
                return reader.ReadByte();
            }
        }

        private class UInt16Writer : IValueWriter<UInt16>
        {
            public void Write(UInt16 value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public UInt16 Read(BinaryReader reader)
            {
                return reader.ReadUInt16();
            }
        }

        private class UInt32Writer : IValueWriter<UInt32>
        {
            public void Write(UInt32 value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public UInt32 Read(BinaryReader reader)
            {
                return reader.ReadUInt32();
            }
        }

        private class UInt64Writer : IValueWriter<UInt64>
        {
            public void Write(UInt64 value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public UInt64 Read(BinaryReader reader)
            {
                return reader.ReadUInt64();
            }
        }

        private class DecimalWriter : IValueWriter<Decimal>
        {
            public void Write(Decimal value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public Decimal Read(BinaryReader reader)
            {
                return reader.ReadDecimal();
            }
        }

        private class SingleWriter : IValueWriter<Single>
        {
            public void Write(Single value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public Single Read(BinaryReader reader)
            {
                return reader.ReadSingle();
            }
        }

        private class DoubleWriter : IValueWriter<Double>
        {
            public void Write(Double value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public Double Read(BinaryReader reader)
            {
                return reader.ReadDouble();
            }
        }

        private class StringWriter : IValueWriter<String>
        {
            public void Write(String value, System.IO.BinaryWriter writer)
            {
                writer.Write(value);
            }

            public String Read(BinaryReader reader)
            {
                return reader.ReadString();
            }
        }

        #endregion

        private static Hashtable _writers = new Hashtable(); // Using hashtable (not Dictionary<T>) because it doesn't need a lock on reads, only writes.

        /// <summary>
        /// Gets a writer for the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>An implementation of <see cref="IValueWriter{T}"/> for the specified type, or <see langword="null"/> if the type implements <see cref="IWritable"/>.</returns>
        /// <exception cref="ArgumentException">There is not writer for <paramref name="type"/> and the type does not implement <see cref="IWritable"/>.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        public static object GetWriter(Type type)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( type.GetInterfaces().Contains(typeof(IWritable)) )
                return null;

            object writer = _writers[type];
            if( writer == null )
            {
                if( type == typeof(int) )
                    writer = new Int32Writer();
                else if( type == typeof(long) )
                    writer = new Int64Writer();
                else if( type == typeof(String) )
                    writer = new StringWriter();
                else if( type == typeof(Single) )
                    writer = new SingleWriter();
                else if( type == typeof(Double) )
                    writer = new DoubleWriter();
                else if( type == typeof(SByte) )
                    writer = new SByteWriter();
                else if( type == typeof(Int16) )
                    writer = new Int16Writer();
                else if( type == typeof(Byte) )
                    writer = new ByteWriter();
                else if( type == typeof(UInt16) )
                    writer = new UInt16Writer();
                else if( type == typeof(UInt32) )
                    writer = new UInt32Writer();
                else if( type == typeof(UInt64) )
                    writer = new UInt64Writer();
                else if( type == typeof(Decimal) )
                    writer = new DecimalWriter();
                else
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Could not find the writer for type {0} and the type does not implement IWritable.", type));

                _writers.Add(type, writer);
            }

            return writer;
        }
    }
}
