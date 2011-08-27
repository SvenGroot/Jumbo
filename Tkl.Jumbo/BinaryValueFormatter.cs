// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Tkl.Jumbo
{
    static class BinaryValueFormatter
    {
        private static Regex _formatRegex = new Regex(@"(?<before>\s*)(?<prefix>[ASKMGTP])?(?<iec>i?)(?<after>B?\s*)$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public static string Format(BinaryValue value, string format, IFormatProvider provider)
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
                    factor = BinaryValue.Byte;
                }
                else if( prefix == "A" || prefix == "a" )
                    factor = DetermineAutomaticScalingFactor(value, false, out realPrefix);
                else if( prefix == "S" || prefix == "s" )
                    factor = DetermineAutomaticScalingFactor(value, true, out realPrefix);
                else
                {
                    realPrefix = prefix;
                    factor = BinaryValue.GetUnitScalingFactor(prefix);
                }

                if( prefix != null && char.IsLower(prefix, 0) )
                    realPrefix = realPrefix.ToLowerInvariant();

                if( factor > 1 )
                    realPrefix += iec;
            }

            return (value.Value / (decimal)factor).ToString(numberFormat, provider) + before + realPrefix + after;
        }

        private static long DetermineAutomaticScalingFactor(BinaryValue value, bool allowRounding, out string prefix)
        {
            if( value >= BinaryValue.Petabyte && (allowRounding || value.Value % BinaryValue.Petabyte == 0) )
            {
                prefix = "P";
                return BinaryValue.Petabyte;
            }
            else if( value >= BinaryValue.Terabyte && (allowRounding || value.Value % BinaryValue.Terabyte == 0) )
            {
                prefix = "T";
                return BinaryValue.Terabyte;
            }
            else if( value >= BinaryValue.Gigabyte && (allowRounding || value.Value % BinaryValue.Gigabyte == 0) )
            {
                prefix = "G";
                return BinaryValue.Gigabyte;
            }
            else if( value >= BinaryValue.Megabyte && (allowRounding || value.Value % BinaryValue.Megabyte == 0) )
            {
                prefix = "M";
                return BinaryValue.Megabyte;
            }
            else if( value >= BinaryValue.Kilobyte && (allowRounding || value.Value % BinaryValue.Kilobyte == 0) )
            {
                prefix = "K";
                return BinaryValue.Kilobyte;
            }
            else
            {
                prefix = "";
                return BinaryValue.Byte;
            }
        }
    }
}
