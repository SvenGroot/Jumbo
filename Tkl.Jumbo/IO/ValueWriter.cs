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
    /// Provides access to <see cref="IValueWriter{T}"/> implementations for various basic framework types and types that specify the <see cref="ValueWriterAttribute"/> attribute.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   If you attempt to access this class for a type <typeparam name="T" /> that does not implement <see cref="IWritable"/> and that does not have a implementation of <see cref="IValueWriter{T}"/>,
    ///   an <see cref="NotSupportedException"/> is thrown by the static type initializer of the <see cref="ValueWriter{T}"/> class.
    /// </para>
    /// </remarks>
    public static class ValueWriter<T>
    {
        #region Nested types

        private class SByteWriter : IValueWriter<SByte>
        {
            public void Write(SByte value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public SByte Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadSByte();
            }
        }

        private class Int16Writer : IValueWriter<Int16>
        {
            public void Write(Int16 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Int16 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadInt16();
            }
        }

        private class Int32Writer : IValueWriter<Int32>
        {
            public void Write(int value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public int Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadInt32();
            }
        }

        private class Int64Writer : IValueWriter<Int64>
        {
            public void Write(Int64 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Int64 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadInt64();
            }
        }

        private class ByteWriter : IValueWriter<Byte>
        {
            public void Write(Byte value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Byte Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadByte();
            }
        }

        private class UInt16Writer : IValueWriter<UInt16>
        {
            public void Write(UInt16 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public UInt16 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadUInt16();
            }
        }

        private class UInt32Writer : IValueWriter<UInt32>
        {
            public void Write(UInt32 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public UInt32 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadUInt32();
            }
        }

        private class UInt64Writer : IValueWriter<UInt64>
        {
            public void Write(UInt64 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public UInt64 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadUInt64();
            }
        }

        private class DecimalWriter : IValueWriter<Decimal>
        {
            public void Write(Decimal value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Decimal Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadDecimal();
            }
        }

        private class SingleWriter : IValueWriter<Single>
        {
            public void Write(Single value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Single Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadSingle();
            }
        }

        private class DoubleWriter : IValueWriter<Double>
        {
            public void Write(Double value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Double Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadDouble();
            }
        }

        private class StringWriter : IValueWriter<String>
        {
            public void Write(String value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public String Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadString();
            }
        }

        private class DateTimeWriter : IValueWriter<DateTime>
        {
            public void Write(DateTime value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write((int)value.Kind);
                writer.Write(value.Ticks);
            }

            public DateTime Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                DateTimeKind kind = (DateTimeKind)reader.ReadInt32();
                long ticks = reader.ReadInt64();
                return new DateTime(ticks, kind);
            }
        }

        #endregion

        private static readonly IValueWriter<T> _writer = (IValueWriter<T>)GetWriter();

        /// <summary>
        /// Gets the writer for the type, or <see langword="null"/> if it implements <see cref="IWritable"/>.
        /// </summary>
        /// <value>
        /// An implementation of <see cref="IValueWriter{T}"/>, or <see langword="null"/> if it implements <see cref="IWritable"/>.
        /// </value>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public static IValueWriter<T> Writer
        {
            get { return _writer; }
        }

        private static object GetWriter()
        {
            Type type = typeof(T);
            if( type.GetInterfaces().Contains(typeof(IWritable)) )
                return null;
            ValueWriterAttribute attribute = (ValueWriterAttribute)Attribute.GetCustomAttribute(type, typeof(ValueWriterAttribute));
            if( attribute != null && !string.IsNullOrEmpty(attribute.ValueWriterTypeName) )
            {
                Type writerType = Type.GetType(attribute.ValueWriterTypeName, true);
                return (IValueWriter<T>)Activator.CreateInstance(writerType);
            }

            if( type == typeof(int) )
                return new Int32Writer();
            else if( type == typeof(long) )
                return new Int64Writer();
            else if( type == typeof(String) )
                return new StringWriter();
            else if( type == typeof(Single) )
                return new SingleWriter();
            else if( type == typeof(Double) )
                return new DoubleWriter();
            else if( type == typeof(SByte) )
                return new SByteWriter();
            else if( type == typeof(Int16) )
                return new Int16Writer();
            else if( type == typeof(Byte) )
                return new ByteWriter();
            else if( type == typeof(UInt16) )
                return new UInt16Writer();
            else if( type == typeof(UInt32) )
                return new UInt32Writer();
            else if( type == typeof(UInt64) )
                return new UInt64Writer();
            else if( type == typeof(Decimal) )
                return new DecimalWriter();
            else if( type == typeof(DateTime) )
                return new DateTimeWriter();
            else
                throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Could not find the writer for type {0} and the type does not implement IWritable.", type));
        }
    }
}
