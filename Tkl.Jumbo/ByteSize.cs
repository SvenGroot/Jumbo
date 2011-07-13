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
    /// Represents a size, expressed in bytes, that can be converted to and from a <see cref="String"/> using binary multiples (e.g. MB).
    /// </summary>
    [TypeConverter(typeof(ByteSizeConverter))]
    public struct ByteSize : IEquatable<ByteSize>, IComparable<ByteSize>, IComparable
    {
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
        public ByteSize(long value)
        {
            _value = value;
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
            get { return _value / (double)Kilobyte; }
        }

        /// <summary>
        /// Gets the value of this instance in gigabytes.
        /// </summary>
        public double InGigabytes
        {
            get { return _value / (double)Kilobyte; }
        }

        /// <summary>
        /// Gets the value of this instance in terabytes.
        /// </summary>
        public double InTerabytes
        {
            get { return _value / (double)Kilobyte; }
        }

        /// <summary>
        /// Gets the value of this instance in petabytes.
        /// </summary>
        public double InPetabytes
        {
            get { return _value / (double)Kilobyte; }
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
        /// Converts the specified <see cref="ByteSize"/> to an <see cref="Int64"/>.
        /// </summary>
        /// <param name="value">The <see cref="ByteSize"/> to convert.</param>
        /// <returns>The value of the <see cref="ByteSize"/> in bytes.</returns>
        public static implicit operator long(ByteSize value)
        {
            return value.Value;
        }

        /// <summary>
        /// Converts a <see cref="Int64"/> to a <see cref="ByteSize"/>.
        /// </summary>
        /// <param name="value">The <see cref="Int64"/> to convert.</param>
        /// <returns>A <see cref="ByteSize"/> with <see cref="Value"/> set to <paramref name="value"/></returns>
        public static implicit operator ByteSize(long value)
        {
            return new ByteSize(value);
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
                size *= GetSuffixMultiplicationFactor(suffix);

            return new ByteSize((long)size);
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
        /// Converts the numeric value of this instance to its equivalent string representation.
        /// </summary>
        /// <returns>The string representation of the value of this instance.</returns>
        public override string ToString()
        {
            return Value.ToString(CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation using the specified culture-specific formatting options.
        /// </summary>
        /// <param name="provider">An <see cref="IFormatProvider"/> that supplies culture-specific formatting information. May be <see langword="null"/> to use the current culture.</param>
        /// <returns>The string representation of the value of this instance as specified by <paramref name="provider"/>.</returns>
        public string ToString(IFormatProvider provider)
        {
            return Value.ToString(provider);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation, scaled according to the specified suffix and using the specified format and culture-specific formatting information.
        /// </summary>
        /// <param name="format">A numeric format string. May be <see langword="null"/>.</param>
        /// <param name="suffix">The binary multiple suffix indicating the scale of the number (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P). May be <see langword="null"/>.</param>
        /// <param name="provider">An <see cref="IFormatProvider"/> that supplies culture-specific formatting information. May be <see langword="null"/> to use the current culture.</param>
        /// <returns>The string representation of the value of this instance as specified by <paramref name="format"/> and <paramref name="provider"/>, with <paramref name="suffix"/> appended.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToString(string format, string suffix, IFormatProvider provider)
        {
            Decimal value = Value;
            if( !string.IsNullOrEmpty(suffix) )
                value /= GetSuffixMultiplicationFactor(suffix.Trim());

            return value.ToString(format, provider) + suffix;
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation, scaled according to the specified suffix and using the specified format.
        /// </summary>
        /// <param name="format">A numeric format string. May be <see langword="null"/>.</param>
        /// <param name="suffix">The binary multiple suffix indicating the scale of the number (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P). May be <see langword="null"/>.</param>
        /// <returns>The string representation of the value of this instance as specified by <paramref name="format"/>, with <paramref name="suffix"/> appended.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToString(string format, string suffix)
        {
            return ToString(format, suffix, CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation, scaled according to the specified suffix and using the specified culture-specific formatting information.
        /// </summary>
        /// <param name="suffix">The binary multiple suffix indicating the scale of the number (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P). May be <see langword="null"/>.</param>
        /// <param name="provider">An <see cref="IFormatProvider"/> that supplies culture-specific formatting information. May be <see langword="null"/> to use the current culture.</param>
        /// <returns>The string representation of the value of this instance as specified by <paramref name="provider"/>, with <paramref name="suffix"/> appended.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToString(string suffix, IFormatProvider provider)
        {
            return ToString(null, suffix, provider);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation, scaled according to the specified suffix.
        /// </summary>
        /// <param name="suffix">The binary multiple suffix indicating the scale of the number (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P). May be <see langword="null"/>.</param>
        /// <returns>The string representation of the value of this instance, with <paramref name="suffix"/> appended.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToString(string suffix)
        {
            return ToString(null, suffix, CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation using the largest binary multiple possible, using the specified format and culture-specific formatting options.
        /// </summary>
        /// <param name="format">A numeric format string. May be <see langword="null"/>.</param>
        /// <param name="suffixOptions">A combination of <see cref="ByteSizeSuffixOptions"/> values indicating how to format the scale suffix.</param>
        /// <param name="provider">An <see cref="IFormatProvider"/> that supplies culture-specific formatting information. May be <see langword="null"/> to use the current culture.</param>
        /// <returns>The string representation of the value of this instance as specified by <paramref name="format"/> and <paramref name="provider"/>, with the appropriate suffix appended as specified by <paramref name="suffixOptions"/>.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToShortString(string format, ByteSizeSuffixOptions suffixOptions, IFormatProvider provider)
        {
            Decimal size = Value;
            string suffix = "";
            if( Value > Petabyte )
            {
                size = Value / (Decimal)Petabyte;
                suffix = "P";
            }
            else if( Value > Terabyte )
            {
                size = Value / (Decimal)Terabyte;
                suffix = "T";
            }
            else if( Value > Gigabyte )
            {
                size = Value / (Decimal)Gigabyte;
                suffix = "G";
            }
            else if( Value > Megabyte )
            {
                size = Value / (Decimal)Megabyte;
                suffix = "M";
            }
            else if( Value > Kilobyte )
            {
                size = Value / (Decimal)Kilobyte;
                suffix = "K";
            }

            if( (suffixOptions & ByteSizeSuffixOptions.UseIecSymbols) == ByteSizeSuffixOptions.UseIecSymbols )
                suffix += "i";
            if( (suffixOptions & ByteSizeSuffixOptions.ExcludeBytes) != ByteSizeSuffixOptions.ExcludeBytes )
                suffix += "B";
            if( (suffixOptions & ByteSizeSuffixOptions.LeadingSpace) == ByteSizeSuffixOptions.LeadingSpace && suffix.Length > 0 )
                suffix = " " + suffix;

            return size.ToString(format, provider) + suffix;
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation using the largest binary multiple possible, using the specified format.
        /// </summary>
        /// <param name="format">A numeric format string. May be <see langword="null"/>.</param>
        /// <param name="suffixOptions">A combination of <see cref="ByteSizeSuffixOptions"/> values indicating how to format the scale suffix.</param>
        /// <returns>The string representation of the value of this instance as specified by <paramref name="format"/>, with the appropriate suffix appended as specified by <paramref name="suffixOptions"/>.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToShortString(string format, ByteSizeSuffixOptions suffixOptions)
        {
            return ToShortString(format, suffixOptions, CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation using the largest binary multiple possible.
        /// </summary>
        /// <param name="suffixOptions">A combination of <see cref="ByteSizeSuffixOptions"/> values indicating how to format the scale suffix.</param>
        /// <returns>The string representation of the value of this instance with the appropriate suffix appended as specified by <paramref name="suffixOptions"/>.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToShortString(ByteSizeSuffixOptions suffixOptions)
        {
            return ToShortString(null, suffixOptions, CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Converts the numeric value of this instance to its equivalent string representation using the largest binary multiple possible.
        /// </summary>
        /// <returns>The string representation of the value of this instance, with the appropriate suffix appended.</returns>
        /// <remarks>
        /// <para>
        ///   The resulting string may contain a rounded number, depending on the scale and the formatting options used.
        /// </para>
        /// </remarks>
        public string ToShortString()
        {
            return ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.CurrentCulture);
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

        private static long GetSuffixMultiplicationFactor(string suffix)
        {
            switch( suffix.ToUpperInvariant() )
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
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Unrecognized suffix {0}.", suffix), "suffix");
            }
        }
    }
}
