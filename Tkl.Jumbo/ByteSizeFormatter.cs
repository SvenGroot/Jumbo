// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Tkl.Jumbo
{
    static class ByteSizeFormatter
    {
        private static Regex _formatRegex = new Regex(@"(?<before>\s*)(?<prefix>[ASKMGTP])?(?<iec>i?)(?<after>B?\s*)$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public static string Format(ByteSize value, string format, IFormatProvider provider)
        {
            string before = null;
            string realPrefix;
            string after = null;
            string numberFormat = null;
            long factor;

            if( string.IsNullOrEmpty(format) )
            {
                factor = DetermineAutomaticScalingFactor(value, false, out realPrefix);
                after = "B";
            }
            else
            {
                Match m = _formatRegex.Match(format);
                if( !m.Success )
                    throw new FormatException("Invalid format string.");

                before = m.Groups["before"].Value;
                string prefix = m.Groups["prefix"].Success ? m.Groups["prefix"].Value : null;
                string iec = m.Groups["iec"].Value;
                after = m.Groups["after"].Value;
                numberFormat = format.Substring(0, m.Index);

                if( prefix == null )
                {
                    realPrefix = null;
                    factor = ByteSize.Byte;
                }
                else if( prefix == "A" || prefix == "a" )
                    factor = DetermineAutomaticScalingFactor(value, false, out realPrefix);
                else if( prefix == "S" || prefix == "s" )
                    factor = DetermineAutomaticScalingFactor(value, true, out realPrefix);
                else
                {
                    realPrefix = prefix;
                    factor = ByteSize.GetUnitScalingFactor(prefix);
                }

                if( prefix != null && char.IsLower(prefix, 0) )
                    realPrefix = realPrefix.ToLowerInvariant();

                if( factor > 1 )
                    realPrefix += iec;
            }

            return (value.Value / (decimal)factor).ToString(numberFormat, provider) + before + realPrefix + after;
        }

        private static long DetermineAutomaticScalingFactor(ByteSize value, bool allowRounding, out string prefix)
        {
            if( value >= ByteSize.Petabyte && (allowRounding || value.Value % ByteSize.Petabyte == 0) )
            {
                prefix = "P";
                return ByteSize.Petabyte;
            }
            else if( value >= ByteSize.Terabyte && (allowRounding || value.Value % ByteSize.Terabyte == 0) )
            {
                prefix = "T";
                return ByteSize.Terabyte;
            }
            else if( value >= ByteSize.Gigabyte && (allowRounding || value.Value % ByteSize.Gigabyte == 0) )
            {
                prefix = "G";
                return ByteSize.Gigabyte;
            }
            else if( value >= ByteSize.Megabyte && (allowRounding || value.Value % ByteSize.Megabyte == 0) )
            {
                prefix = "M";
                return ByteSize.Megabyte;
            }
            else if( value >= ByteSize.Kilobyte && (allowRounding || value.Value % ByteSize.Kilobyte == 0) )
            {
                prefix = "K";
                return ByteSize.Kilobyte;
            }
            else
            {
                prefix = "";
                return ByteSize.Byte;
            }
        }
    }
}
