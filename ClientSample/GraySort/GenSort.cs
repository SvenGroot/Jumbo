using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClientSample.GraySort
{
    public class GenSort
    {
        private const int _recordSize = 100;
        private byte[] _recordBuffer = new byte[_recordSize];

        public IEnumerable<byte[]> GenerateRecords(UInt128 startRecord, ulong count)
        {
            Random128 rnd = new Random128(startRecord);
            UInt128 recordNumber = startRecord;
            for( ulong x = 0; x < count; ++x )
            {
                GenerateAsciiRecord(rnd.Next(), recordNumber);
                yield return _recordBuffer;
                ++recordNumber;
            }
        }

        private void GenerateAsciiRecord(UInt128 random, UInt128 recordNumber)
        {
            int i;
            ulong temp;

            /* generate the 10-byte ascii key using mostly the high 64 bits.
             */
            temp = random.High64;
            _recordBuffer[0] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[1] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[2] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[3] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[4] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[5] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[6] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[7] = (byte)(' ' + (temp % 95));
            temp = random.Low64;
            _recordBuffer[8] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _recordBuffer[9] = (byte)(' ' + (temp % 95));
            temp /= 95;

            /* add 2 bytes of "break" */
            _recordBuffer[10] = (byte)' ';
            _recordBuffer[11] = (byte)' ';

            /* convert the 128-bit record number to 32 bits of ascii hexadecimal
             * as the next 32 bytes of the record.
             */
            for( i = 0; i < 16; i++ )
                _recordBuffer[12 + i] = HexDigit((recordNumber.High64 >> (60 - 4 * i)) & 0xF);
            for( i = 0; i < 16; i++ )
                _recordBuffer[28 + i] = HexDigit((recordNumber.Low64 >> (60 - 4 * i)) & 0xF);

            /* add 2 bytes of "break" data */
            _recordBuffer[44] = (byte)' ';
            _recordBuffer[45] = (byte)' ';

            /* add 52 bytes of filler based on low 48 bits of randomom number */
            _recordBuffer[46] = _recordBuffer[47] = _recordBuffer[48] = _recordBuffer[49] =
                HexDigit((random.Low64 >> 48) & 0xF);
            _recordBuffer[50] = _recordBuffer[51] = _recordBuffer[52] = _recordBuffer[53] =
                HexDigit((random.Low64 >> 44) & 0xF);
            _recordBuffer[54] = _recordBuffer[55] = _recordBuffer[56] = _recordBuffer[57] =
                HexDigit((random.Low64 >> 40) & 0xF);
            _recordBuffer[58] = _recordBuffer[59] = _recordBuffer[60] = _recordBuffer[61] =
                HexDigit((random.Low64 >> 36) & 0xF);
            _recordBuffer[62] = _recordBuffer[63] = _recordBuffer[64] = _recordBuffer[65] =
                HexDigit((random.Low64 >> 32) & 0xF);
            _recordBuffer[66] = _recordBuffer[67] = _recordBuffer[68] = _recordBuffer[69] =
                HexDigit((random.Low64 >> 28) & 0xF);
            _recordBuffer[70] = _recordBuffer[71] = _recordBuffer[72] = _recordBuffer[73] =
                HexDigit((random.Low64 >> 24) & 0xF);
            _recordBuffer[74] = _recordBuffer[75] = _recordBuffer[76] = _recordBuffer[77] =
                HexDigit((random.Low64 >> 20) & 0xF);
            _recordBuffer[78] = _recordBuffer[79] = _recordBuffer[80] = _recordBuffer[81] =
                HexDigit((random.Low64 >> 16) & 0xF);
            _recordBuffer[82] = _recordBuffer[83] = _recordBuffer[84] = _recordBuffer[85] =
                HexDigit((random.Low64 >> 12) & 0xF);
            _recordBuffer[86] = _recordBuffer[87] = _recordBuffer[88] = _recordBuffer[89] =
                HexDigit((random.Low64 >> 8) & 0xF);
            _recordBuffer[90] = _recordBuffer[91] = _recordBuffer[92] = _recordBuffer[93] =
                HexDigit((random.Low64 >> 4) & 0xF);
            _recordBuffer[94] = _recordBuffer[95] = _recordBuffer[96] = _recordBuffer[97] =
                HexDigit((random.Low64 >> 0) & 0xF);

            /* add 2 bytes of "break" data */
            _recordBuffer[98] = (byte)'\r';	/* nice for Windows */
            _recordBuffer[99] = (byte)'\n';            
        }

        private static byte HexDigit(ulong value)
        {
            return (byte)(value >= 10 ? 'A' + value - 10 : '0' + value);
        }
    }
}
