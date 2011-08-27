// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.ComponentModel;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides formatting, parsing and scaling for a value using binary units (e.g. MB).
    /// </summary>
    [TypeConverter(typeof(ByteSizeConverter))]
    public struct ByteSize : IEquatable<ByteSize>, IComparable<ByteSize>, IComparable, IFormattable, IConvertible
    {
        /// <summary>
        /// The size of a byte, 1 byte.
        /// </summary>
        public const long Byte = 1L;
        /// <summary>
        /// The size of a kilobyte, 1024 bytes.
        /// </summary>
        public const long Kilobyte = 1024L;
        /// <summary>
        /// The size of a megabyte, 1048576 bytes.
        /// </summary>
        public const long Megabyte = 1024L * 1024L;
        /// <summary>
        /// The size of a gigabyte, 1073741824 bytes.
        /// </summary>
        public const long Gigabyte = 1024L * 1024L * 1024L;
        /// <summary>
        /// The size of a TeraByte, 1099511627776 bytes.
        /// </summary>
        public const long Terabyte = 1024L * 1024L * 1024L * 1024L;
        /// <summary>
        /// The size of a PetaByte, 1125899906842624 bytes.
        /// </summary>
        public const long Petabyte = 1024L * 1024L * 1024L * 1024L * 1024L;


        private static readonly char[] _numbers = new[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
        private static readonly ByteSize _zero = new ByteSize();
        private readonly long _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteSize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        public ByteSize(int value)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteSize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        [CLSCompliant(false)]
        public ByteSize(uint value)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteSize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        public ByteSize(long value)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteSize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        [CLSCompliant(false)]
        public ByteSize(ulong value)
        {
            checked
            {
                _value = (long)value;
            }
        }

        /// <summary>
        /// Gets the value of this instance, in bytes.
        /// </summary>
        public long Value
        {
            get { return _value; }
        }

        /// <summary>
        /// Gets the value of this instance in kilobytes.
        /// </summary>
        public double InKilobytes
        {
            get { return _value / (double)Kilobyte; }
        }

        /// <summary>
        /// Gets the value of this instance in megabytes.
        /// </summary>
        public double InMegabytes
        {
            get { return _value / (double)Megabyte; }
        }

        /// <summary>
        /// Gets the value of this instance in gigabytes.
        /// </summary>
        public double InGigabytes
        {
            get { return _value / (double)Gigabyte; }
        }

        /// <summary>
        /// Gets the value of this instance in terabytes.
        /// </summary>
        public double InTerabytes
        {
            get { return _value / (double)Terabyte; }
        }

        /// <summary>
        /// Gets the value of this instance in petabytes.
        /// </summary>
        public double InPetabytes
        {
            get { return _value / (double)Petabyte; }
        }

        /// <summary>
        /// Gets a zero-valued <see cref="ByteSize"/> instance.
        /// </summary>
        /// <value>A <see cref="ByteSize"/> instance with <see cref="Value"/> set to zero.</value>
        public static ByteSize Zero
        {
            get { return _zero; }
        }

        /// <summary>
        /// Converts the string representation of a byte size in a specified culture-specific format into a <see cref="ByteSize"/> structure.
        /// </summary>
        /// <param name="value">A string containing a number to convert. This string may use a suffix indicating a binary multiple (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P).</param>
        /// <param name="provider">An <see cref="IFormatProvider"/> that supplies culture-specific formatting information about <paramref name="value" />. May be <see langword="null"/> to use the current culture.</param>
        /// <returns>A <see cref="ByteSize"/> instance that is the equivalent of <paramref name="value"/>.</returns>
        public static ByteSize Parse(string value, IFormatProvider provider)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            if( value.Length == 0 )
                return new ByteSize();

            string suffix = GetAndRemoveSuffix(ref value);
            Decimal size = Decimal.Parse(value, provider);
            if( suffix != null )
                size *= GetUnitScalingFactor(suffix);

            checked
            {
                return new ByteSize((long)size);
            }
        }

        /// <summary>
        /// Converts the string representation of a byte size into a <see cref="ByteSize"/> structure.
        /// </summary>
        /// <param name="value">A string containing a number to convert. This string may use a suffix indicating a binary multiple (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P).</param>
        /// <returns>A <see cref="ByteSize"/> instance that is the equivalent of <paramref name="value"/>.</returns>
        public static ByteSize Parse(string value)
        {
            return Parse(value, CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="formatProvider">The format provider.</param>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   The value of <paramref name="format"/> must be a string containing a numeric format string followed by a binary unit, or either one of both. If no numeric
        ///   format is present, the default is used. If no binary unit is specified, the raw value in bytes is used.
        /// </para>
        /// <para>
        ///   The first character of the binary suffix indicates the scaling factor. This can be one of the normal binary prefixes K, M, G, T, or P. The value A (auto) indicates that
        ///   the scaling factor should be automatically determined as the largest factor in which this value can be precisely represented with no decimals. The value S (short)
        ///   indicates that the scaling factor should be automatically determined as the largest possible scaling factor in which this value can be represented with the scaled
        ///   value being at least 1. Using S may lead to rounding so while this is appropriate for some display scenarios, it is not appropriate if the precise value must be preserved.
        /// </para>
        /// <para>
        ///   The binary prefix can be followed by either B or iB, which will be included in the unit of the output.
        /// </para>
        /// <para>
        ///   The casing of the binary unit will be preserved as in the format string. Any whitespace that surrounding the binary unit will be preserved.
        /// </para>
        /// </remarks>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return ByteSizeFormatter.Format(this, format, formatProvider);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <param name="format">The format.</param>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   The value of <paramref name="format"/> must be a string containing a numeric format string followed by a binary unit, or either one of both. If no numeric
        ///   format is present, the default is used. If no binary unit is specified, the raw value in bytes is used.
        /// </para>
        /// <para>
        ///   The first character of the binary suffix indicates the scaling factor. This can be one of the normal binary prefixes K, M, G, T, or P. The value A (auto) indicates that
        ///   the scaling factor should be automatically determined as the largest factor in which this value can be precisely represented with no decimals. The value S (short)
        ///   indicates that the scaling factor should be automatically determined as the largest possible scaling factor in which this value can be represented with the scaled
        ///   value being at least 1. Using S may lead to rounding so while this is appropriate for some display scenarios, it is not appropriate if the precise value must be preserved.
        /// </para>
        /// <para>
        ///   The binary prefix can be followed by either B or iB to indicate the the unit formatting.
        /// </para>
        /// <para>
        ///   The casing of the binary unit will be preserved as in the format string. Any whitespace that surrounding the binary unit will be preserved.
        /// </para>
        /// </remarks>
        public string ToString(string format)
        {
            return ByteSizeFormatter.Format(this, format, null);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <param name="formatProvider">The format provider.</param>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public string ToString(IFormatProvider formatProvider)
        {
            return ByteSizeFormatter.Format(this, null, formatProvider);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return ByteSizeFormatter.Format(this, null, null);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified object.
        /// </summary>
        /// <param name="obj">The object to compare to this instance.</param>
        /// <returns><see langword="true"/> if <paramref name="obj"/> has the same value as this instance; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            if( obj is ByteSize )
                return Equals((ByteSize)obj);
            else
                return false;
        }

        /// <summary>
        /// Returns the hash code for this <see cref="ByteSize"/>.
        /// </summary>
        /// <returns>The hash code for this <see cref="ByteSize"/>.</returns>
        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        /// <summary>
        /// Determines whether two specified <see cref="ByteSize"/> values are the same.
        /// </summary>
        /// <param name="left">A <see cref="ByteSize"/>.</param>
        /// <param name="right">A <see cref="ByteSize"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(ByteSize left, ByteSize right)
        {
            return left.Value == right.Value;
        }

        /// <summary>
        /// Determines whether two specified <see cref="ByteSize"/> values are different.
        /// </summary>
        /// <param name="left">A <see cref="ByteSize"/>.</param>
        /// <param name="right">A <see cref="ByteSize"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(ByteSize left, ByteSize right)
        {
            return left.Value != right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="ByteSize"/> is less than another <see cref="ByteSize"/>.
        /// </summary>
        /// <param name="left">A <see cref="ByteSize"/>.</param>
        /// <param name="right">A <see cref="ByteSize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <(ByteSize left, ByteSize right)
        {
            return left.Value < right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="ByteSize"/> is less than or equal to another <see cref="ByteSize"/>.
        /// </summary>
        /// <param name="left">A <see cref="ByteSize"/>.</param>
        /// <param name="right">A <see cref="ByteSize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than or equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <=(ByteSize left, ByteSize right)
        {
            return left.Value <= right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="ByteSize"/> is greater than another <see cref="ByteSize"/>.
        /// </summary>
        /// <param name="left">A <see cref="ByteSize"/>.</param>
        /// <param name="right">A <see cref="ByteSize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >(ByteSize left, ByteSize right)
        {
            return left.Value > right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="ByteSize"/> is greater than or equal to another <see cref="ByteSize"/>.
        /// </summary>
        /// <param name="left">A <see cref="ByteSize"/>.</param>
        /// <param name="right">A <see cref="ByteSize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than or equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >=(ByteSize left, ByteSize right)
        {
            return left.Value >= right.Value;
        }

        #region Conversion operators

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.Byte"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator byte(ByteSize value)
        {
            checked
            {
                return (byte)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.SByte"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator sbyte(ByteSize value)
        {
            checked
            {
                return (sbyte)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.Int16"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator short(ByteSize value)
        {
            checked
            {
                return (short)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.UInt16"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator ushort(ByteSize value)
        {
            checked
            {
                return (ushort)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.Int32"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator int(ByteSize value)
        {
            checked
            {
                return (int)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.UInt32"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator uint(ByteSize value)
        {
            checked
            {
                return (uint)value.Value;
            }
        }

        /// <summary>
        /// Converts the specified <see cref="ByteSize"/> to an <see cref="Int64"/>.
        /// </summary>
        /// <param name="value">The <see cref="ByteSize"/> to convert.</param>
        /// <returns>The value of the <see cref="ByteSize"/> in bytes.</returns>
        public static explicit operator long(ByteSize value)
        {
            return value.Value;
        }

        /// <summary>
        /// Converts the specified <see cref="ByteSize"/> to an <see cref="UInt64"/>.
        /// </summary>
        /// <param name="value">The <see cref="ByteSize"/> to convert.</param>
        /// <returns>The value of the <see cref="ByteSize"/> in bytes.</returns>
        [CLSCompliant(false)]
        public static explicit operator ulong(ByteSize value)
        {
            checked
            {
                return (ulong)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.Decimal"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator decimal(ByteSize value)
        {
            checked
            {
                return (decimal)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.Single"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator float(ByteSize value)
        {
            checked
            {
                return (float)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Tkl.Jumbo.ByteSize"/> to <see cref="System.Double"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator double(ByteSize value)
        {
            checked
            {
                return (double)value.Value;
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Byte"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(byte value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.SByte"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator ByteSize(sbyte value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int16"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(short value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt16"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator ByteSize(ushort value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int32"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(int value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt32"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator ByteSize(uint value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int64"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(long value)
        {
            return new ByteSize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt64"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator ByteSize(ulong value)
        {
            checked
            {
                return new ByteSize((long)value);
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Decimal"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(decimal value)
        {
            checked
            {
                return new ByteSize((long)value);
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Single"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(float value)
        {
            checked
            {
                return new ByteSize((long)value);
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Double"/> to <see cref="Tkl.Jumbo.ByteSize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator ByteSize(double value)
        {
            checked
            {
                return new ByteSize((long)value);
            }
        }

        #endregion


        #region IEquatable<ByteSize> Members

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified <see cref="ByteSize"/> value.
        /// </summary>
        /// <param name="other">The <see cref="ByteSize"/> value to compare to this instance.</param>
        /// <returns><see langword="true"/> if <paramref name="other"/> has the same value as this instance; otherwise, <see langword="false"/>.</returns>
        public bool Equals(ByteSize other)
        {
            return object.Equals(Value, other.Value);
        }

        #endregion

        #region IComparable<ByteSize> Members

        /// <summary>
        /// Compares this instance to a specified <see cref="ByteSize"/> and returns an indication of their relative values.
        /// </summary>
        /// <param name="other">A <see cref="ByteSize"/> to compare.</param>
        /// <returns>Less than zero if this instance is less than <paramref name="other"/>, zero if this instance is equal to <paramref name="other"/>, or greater than zero if this instance is greater than <paramref name="other"/>.</returns>
        public int CompareTo(ByteSize other)
        {
            return Value.CompareTo(other.Value);
        }

        #endregion

        #region IComparable Members

        /// <summary>
        /// Compares this instance to a specified object and returns an indication of their relative values.
        /// </summary>
        /// <param name="obj">An object to compare.</param>
        /// <returns>Less than zero if this instance is less than <paramref name="obj"/>, zero if this instance is equal to <paramref name="obj"/>, or greater than zero if this instance is greater than <paramref name="obj"/> or <paramref name="obj"/> is <see langword="null"/>.</returns>
        public int CompareTo(object obj)
        {
            if( obj == null )
                return 1;
            else if( obj is ByteSize )
                return CompareTo((ByteSize)obj);
            else
                throw new ArgumentException("The specified value is not a ByteSize.", "obj");
        }

        #endregion

        #region IConvertable members

        TypeCode IConvertible.GetTypeCode()
        {
            return TypeCode.Object;
        }

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return Convert.ToBoolean(Value, provider);
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return Convert.ToByte(Value, provider);
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            return Convert.ToChar(Value, provider);
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            return Convert.ToDateTime(Value, provider);
        }

        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return Convert.ToDecimal(Value, provider);
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return Convert.ToDouble(Value, provider);
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return Convert.ToInt16(Value, provider);
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return Convert.ToInt32(Value, provider);
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return Convert.ToInt64(Value, provider);
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return Convert.ToSByte(Value, provider);
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return Convert.ToSingle(Value, provider);
        }

        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            if( conversionType == typeof(string) )
                return ToString(provider);
            else
                return Convert.ChangeType(Value, conversionType, provider);
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return Convert.ToUInt16(Value, provider);
        }

        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return Convert.ToUInt32(Value, provider);
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return Convert.ToUInt64(Value, provider);
        }

        #endregion

        internal static long GetUnitScalingFactor(string unit)
        {
            switch( unit.ToUpperInvariant() )
            {
            case "B":
                return 1;
            case "KB":
            case "KIB":
            case "K":
                return Kilobyte;
            case "MB":
            case "MIB":
            case "M":
                return Megabyte;
            case "GB":
            case "GIB":
            case "G":
                return Gigabyte;
            case "TB":
            case "TIB":
            case "T":
                return Terabyte;
            case "PB":
            case "PIB":
            case "P":
                return Petabyte;
            default:
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Unrecognized suffix {0}.", unit), "suffix");
            }
        }

        private static string GetAndRemoveSuffix(ref string value)
        {
            int lastNumber = value.LastIndexOfAny(_numbers);
            if( lastNumber == value.Length - 1 )
                return null;
            else
            {
                string suffix = value.Substring(lastNumber + 1);
                value = value.Substring(0, lastNumber + 1);
                return suffix.Trim();
            }
        }
    }
}
