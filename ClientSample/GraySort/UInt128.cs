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
