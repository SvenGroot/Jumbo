using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Represents the group by clause fields of the output of TPC-H query 1.
    /// </summary>
    public class PricingSummaryKey : Writable<PricingSummaryKey>, IComparable<PricingSummaryKey>, IEquatable<PricingSummaryKey>
    {
        /// <summary>
        /// L_RETURNFLAG, fixed text, size 1
        /// </summary>
        public char ReturnFlag { get; set; }

        /// <summary>
        /// L_LINESTATUS, fixed text, size 1
        /// </summary>
        public char LineStatus { get; set; }

        #region IComparable<PricingSummaryKey> Members

        /// <summary>
        /// IComparable
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public int CompareTo(PricingSummaryKey other)
        {
            if( other == null )
                return 1;
            int result = ReturnFlag - other.ReturnFlag;
            if( result == 0 )
                return LineStatus - other.LineStatus;
            else
                return result;
        }

        #endregion

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as PricingSummaryKey);
        }

        /// <summary>
        /// GetHashCode
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return (int)ReturnFlag << 8 | (int)LineStatus;
        }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{{ReturnFlag={0}, LineStatus={1}}}", ReturnFlag, LineStatus);
        }

        #region IEquatable<PricingSummaryKey> Members

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(PricingSummaryKey other)
        {
            if( other == null )
                return false;
            return ReturnFlag == other.ReturnFlag && LineStatus == other.LineStatus;
        }

        #endregion
    }
}
