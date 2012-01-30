// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides raw comparers for built-in framework types.
    /// </summary>
    static class DefaultRawComparer
    {
        #region Nested types

        private sealed class SByteComparer : IRawComparer<SByte>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                SByte value1 = (SByte)x[xOffset];
                SByte value2 = (SByte)y[yOffset];
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class ByteComparer : IRawComparer<Byte>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Byte value1 = (Byte)x[xOffset];
                Byte value2 = (Byte)y[yOffset];
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class Int16Comparer : IRawComparer<Int16>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Int16 value1 = LittleEndianBitConverter.ToInt16(x, xOffset);
                Int16 value2 = LittleEndianBitConverter.ToInt16(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class UInt16Comparer : IRawComparer<UInt16>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                UInt16 value1 = LittleEndianBitConverter.ToUInt16(x, xOffset);
                UInt16 value2 = LittleEndianBitConverter.ToUInt16(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class Int32Comparer : IRawComparer<Int32>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Int32 value1 = LittleEndianBitConverter.ToInt32(x, xOffset);
                Int32 value2 = LittleEndianBitConverter.ToInt32(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class UInt32Comparer : IRawComparer<UInt32>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                UInt32 value1 = LittleEndianBitConverter.ToUInt32(x, xOffset);
                UInt32 value2 = LittleEndianBitConverter.ToUInt32(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class Int64Comparer : IRawComparer<Int64>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Int64 value1 = LittleEndianBitConverter.ToInt64(x, xOffset);
                Int64 value2 = LittleEndianBitConverter.ToInt64(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class UInt64Comparer : IRawComparer<UInt64>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                UInt64 value1 = LittleEndianBitConverter.ToUInt64(x, xOffset);
                UInt64 value2 = LittleEndianBitConverter.ToUInt64(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class DecimalComparer : IRawComparer<Decimal>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Decimal value1 = LittleEndianBitConverter.ToDecimal(x, xOffset);
                Decimal value2 = LittleEndianBitConverter.ToDecimal(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class SingleComparer : IRawComparer<Single>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Single value1 = LittleEndianBitConverter.ToSingle(x, xOffset);
                Single value2 = LittleEndianBitConverter.ToSingle(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class DoubleComparer : IRawComparer<Double>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                Double value1 = LittleEndianBitConverter.ToDouble(x, xOffset);
                Double value2 = LittleEndianBitConverter.ToDouble(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class DateTimeComparer : IRawComparer<DateTime>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                DateTime value1 = LittleEndianBitConverter.ToDateTime(x, xOffset);
                DateTime value2 = LittleEndianBitConverter.ToDateTime(y, yOffset);
                return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
            }
        }

        private sealed class StringComparer : IRawComparer<string>
        {
            public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
            {
                return RawComparerHelper.CompareBytesWith7BitEncodedLength(x, xOffset, xCount, y, yOffset, yCount);
            }
        }

        #endregion

        public static object GetComparer(Type type)
        {
            if( type == typeof(SByte) )
                return new SByteComparer();
            else if( type == typeof(Byte) )
                return new ByteComparer();
            else if( type == typeof(Int16) )
                return new Int16Comparer();
            else if( type == typeof(UInt16) )
                return new UInt16Comparer();
            else if( type == typeof(Int32) )
                return new Int32Comparer();
            else if( type == typeof(UInt32) )
                return new UInt32Comparer();
            else if( type == typeof(Int64) )
                return new Int64Comparer();
            else if( type == typeof(UInt64) )
                return new UInt64Comparer();
            else if( type == typeof(Decimal) )
                return new DecimalComparer();
            else if( type == typeof(Single) )
                return new SingleComparer();
            else if( type == typeof(Double) )
                return new DoubleComparer();
            else if( type == typeof(DateTime) )
                return new DateTimeComparer();
            else if( type == typeof(String) )
                return new StringComparer();

            return null;
        }
    }
}
