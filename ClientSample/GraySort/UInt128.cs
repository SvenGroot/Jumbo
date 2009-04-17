using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClientSample.GraySort
{
    public struct UInt128
    {
        private readonly ulong _high64;
        private readonly ulong _low64;

        private static ulong[] _hi2loQuot = new ulong[] {
            0UL,
            1844674407370955161UL,
            3689348814741910323UL,
            5534023222112865484UL,
            7378697629483820646UL,
            9223372036854775808UL,
            11068046444225730969UL,
            12912720851596686131UL,
            14757395258967641292UL,
            16602069666338596454UL
        };

        private static int[] _hi2loMod = new int[] {
            0,
            6,
            2,
            8,
            4,
            0,
            6,
            2,
            8,
            4
        };

        public static readonly UInt128 Zero = new UInt128();

        public UInt128(ulong high64, ulong low64)
        {
            _high64 = high64;
            _low64 = low64;
        }

        public ulong High64
        {
            get { return _high64; }
        }

        public ulong Low64
        {
            get { return _low64; }
        }

        public override bool Equals(object obj)
        {
            if( !(obj is UInt128) )
                return false;
            UInt128 other = (UInt128)obj;
            return this == other;
        }

        public override string ToString()
        {
            ulong          hi8 = High64;
            ulong          lo8 = Low64;
            int         himod;
            int         lomod;
            char[] temp = new char[39];
            int digit = 0;

            while (hi8 != 0)
            {
                himod = (int)(hi8 % 10);
                hi8 /= 10;
                lomod = (int)(lo8 % 10);
                lo8 /= 10;

                lo8 += _hi2loQuot[himod] ;
                lomod += _hi2loMod[himod];

                if (lomod >= 10)       /* if adding to 2 mods caused a "carry" */
                {
                    lomod -= 10;
                    lo8 += 1;
                }
                temp[digit++] = (char)('0' + lomod);
            }
            string lowString = lo8.ToString();
            StringBuilder result = new StringBuilder(lowString.Length + digit);
            result.Append(lowString);
            /* concatenate low order digits computed before hi8 was reduced to 0 */
            while( digit > 0 )
                result.Append(temp[--digit]);
            return result.ToString();
        }

        public string ToHexString()
        {
            if( High64 != 0 )
                return High64.ToString("x") + Low64.ToString("x");
            else
                return Low64.ToString("x");
        }

        public override int GetHashCode()
        {
            return High64.GetHashCode() ^ Low64.GetHashCode();
        }

        public static bool operator ==(UInt128 left, UInt128 right)
        {
            return left.High64 == right.High64 && left.Low64 == right.Low64;
        }

        public static bool operator !=(UInt128 left, UInt128 right)
        {
            return !(left.High64 == right.High64 && left.Low64 == right.Low64);
        }

        public static UInt128 operator ++(UInt128 value)
        {
            ulong sumLow = value.Low64 + 1;
            ulong sumHigh = sumLow == 0 ? value.High64 + 1 : value.High64;
            return new UInt128(sumHigh, sumLow);
        }

        public static UInt128 operator +(UInt128 left, UInt128 right)
        {
            ulong sumLow;
            ulong sumHigh;

            ulong resultHighBit;
            ulong highBit0;
            ulong highBit1;

            sumHigh = left.High64 + right.High64;

            highBit0 = (left.Low64 & 0x8000000000000000L);
            highBit1 = (right.Low64 & 0x8000000000000000L);
            sumLow = left.Low64 + right.Low64;
            resultHighBit = (sumLow & 0x8000000000000000L);
            if( (highBit0 & highBit1) != 0L || ((highBit0 ^ highBit1) != 0L && resultHighBit == 0L) )
                ++sumHigh; // add carry
            return new UInt128(sumHigh, sumLow);
        }

        public static UInt128 operator *(UInt128 a, UInt128 b)
        {
            ulong productHigh, productLow;
            ulong ahi4, alow4, bhi4, blow4, temp;
            ulong reshibit, hibit0, hibit1;

            productHigh = 0;

            ahi4 = a.Low64 >> 32;        /* get hi 4 bytes of the low 8 bytes */
            alow4 = (a.Low64 & 0xFFFFFFFFL);  /* get low 4 bytes of the low 8 bytes */
            bhi4 = b.Low64 >> 32;        /* get hi 4 bytes of the low 8 bytes */
            blow4 = (b.Low64 & 0xFFFFFFFFL);  /* get low 4 bytes of the low 8 bytes */

            /* assign 8-byte product of the lower 4 bytes of "a" and the lower 4 bytes
             * of "b" to the lower 8 bytes of the result product.
             */
            productLow = alow4 * blow4;

            temp = ahi4 * blow4; /* mult high 4 bytes of "a" by low 4 bytes of "b" */
            productHigh += temp >> 32; /* add high 4 bytes to high 8 result bytes*/
            temp <<= 32;     /* get lower half ready to add to lower 8 result bytes */
            hibit0 = (temp & 0x8000000000000000L);
            hibit1 = (productLow & 0x8000000000000000L);
            productLow += temp;
            reshibit = (productLow & 0x8000000000000000L);
            if( (hibit0 & hibit1) != 0L || ((hibit0 ^ hibit1) != 0L && reshibit == 0L) )
                productHigh++;  /* add carry bit */

            temp = alow4 * bhi4; /* mult low 4 bytes of "a" by high 4 bytes of "b" */
            productHigh += temp >> 32; /* add high 4 bytes to high 8 result bytes*/
            temp <<= 32;     /* get lower half ready to add to lower 8 result bytes */
            hibit0 = (temp & 0x8000000000000000L);
            hibit1 = (productLow & 0x8000000000000000L);
            productLow += temp;
            reshibit = (productLow & 0x8000000000000000L);
            if( (hibit0 & hibit1) != 0L || ((hibit0 ^ hibit1) != 0L && reshibit == 0L) )
                productHigh++;  /* add carry bit */

            productHigh += ahi4 * bhi4;
            productHigh += a.Low64 * b.High64;
            productHigh += a.High64 * b.Low64;
            return new UInt128(productHigh, productLow);
        }
    }
}
