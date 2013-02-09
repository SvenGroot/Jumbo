// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Represents a record from the LINEITEM table of the TPC-H benchmark.
    /// </summary>
    public sealed class LineItem : Writable<LineItem>, IEquatable<LineItem>
    {
        /// <summary>
        /// L_ORDERKEY, identifier
        /// </summary>
        public int OrderKey { get; set; }
        
        /// <summary>
        /// L_PARTKEY, identifier
        /// </summary>
        public int PartKey { get; set; }

        /// <summary>
        /// L_SUPPKEY, identifier
        /// </summary>
        public int SuppKey { get; set; }

        /// <summary>
        /// L_LINENUMBER, identifier
        /// </summary>
        public int LineNumber { get; set; }

        /// <summary>
        /// L_QUANTITY, decimal
        /// </summary>
        public long Quantity { get; set; }

        /// <summary>
        /// L_EXTENDEDPRICE, decimal
        /// </summary>
        public long ExtendedPrice { get; set; }

        /// <summary>
        /// L_DISCOUNT, decimal
        /// </summary>
        public long Discount { get; set; }

        /// <summary>
        /// L_TAX, decimal
        /// </summary>
        public long Tax { get; set; }

        /// <summary>
        /// L_RETURNFLAG, fixed text, size 1
        /// </summary>
        public char ReturnFlag { get; set; }

        /// <summary>
        /// L_LINESTATUS, fixed text, size 1
        /// </summary>
        public char LineStatus { get; set; }

        /// <summary>
        /// L_SHIPDATE, date
        /// </summary>
        public DateTime ShipDate { get; set; }

        /// <summary>
        /// L_COMMITDATE, date
        /// </summary>
        public DateTime CommitDate { get; set; }

        /// <summary>
        /// L_RECEIPTDATE, date
        /// </summary>
        public DateTime ReceiptDate { get; set; }

        /// <summary>
        /// L_SHIPINSTRUCT, fixed text, size 25
        /// </summary>
        public Utf8String ShipInstruct { get; set; }

        /// <summary>
        /// L_SHIPMODE, fixed text, size 10
        /// </summary>
        public Utf8String ShipMode { get; set; }

        /// <summary>
        /// L_COMMENT, variable text, size 44
        /// </summary>
        public Utf8String Comment { get; set; }

        /// <summary>
        /// Reads the <see cref="LineItem"/> from a record produced by dbgen.
        /// </summary>
        /// <param name="item">The dbgen LINEITEM record.</param>
        public void FromString(string item)
        {
            const string dateFormat = "yyyy-MM-dd";

            if( item == null )
                throw new ArgumentNullException("item");

            string[] fields = item.Split('|');

            OrderKey = Convert.ToInt32(fields[0], System.Globalization.CultureInfo.InvariantCulture);
            PartKey = Convert.ToInt32(fields[1], System.Globalization.CultureInfo.InvariantCulture);
            SuppKey = Convert.ToInt32(fields[2], System.Globalization.CultureInfo.InvariantCulture);
            LineNumber = Convert.ToInt32(fields[3], System.Globalization.CultureInfo.InvariantCulture);
            Quantity = (long)(Convert.ToDecimal(fields[4], System.Globalization.CultureInfo.InvariantCulture));
            ExtendedPrice = (long)(Convert.ToDecimal(fields[5], System.Globalization.CultureInfo.InvariantCulture) * 100);
            Discount = (long)(Convert.ToDecimal(fields[6], System.Globalization.CultureInfo.InvariantCulture) * 100);
            Tax = (long)(Convert.ToDecimal(fields[7], System.Globalization.CultureInfo.InvariantCulture) * 100);
            ReturnFlag = fields[8][0];
            LineStatus = fields[9][0];
            ShipDate = DateTime.ParseExact(fields[10], dateFormat, System.Globalization.CultureInfo.InvariantCulture);
            CommitDate = DateTime.ParseExact(fields[11], dateFormat, System.Globalization.CultureInfo.InvariantCulture);
            ReceiptDate = DateTime.ParseExact(fields[12], dateFormat, System.Globalization.CultureInfo.InvariantCulture);
            if( ShipInstruct == null )
                ShipInstruct = new Utf8String(fields[13]);
            else
                ShipInstruct.Set(fields[13]);
            if( ShipMode == null )
                ShipMode = new Utf8String(fields[14]);
            else
                ShipMode.Set(fields[14]);
            if( Comment == null )
                Comment = new Utf8String(fields[15]);
            else
                Comment.Set(fields[15]);
        }

        /// <summary>
        /// Tests this <see cref="LineItem"/> for equality with the specified object.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to test for equality.</param>
        /// <returns><see langword="true"/> if this instance is equal to <paramref name="obj"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as LineItem);
        }

        /// <summary>
        /// Overrides <see cref="object.GetHashCode"/>.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        #region IEquatable<LineItem> Members

        /// <summary>
        /// Tests this <see cref="LineItem"/> for equality with the specified <see cref="LineItem"/>.
        /// </summary>
        /// <param name="other">The <see cref="LineItem"/> to test for equality.</param>
        /// <returns><see langword="true"/> if this instance is equal to <paramref name="other"/>; otherwise, <see langword="false"/>.</returns>
        public bool Equals(LineItem other)
        {
            if( other == null )
                return false;

            return OrderKey == other.OrderKey && PartKey == other.PartKey && SuppKey == other.SuppKey && LineNumber == other.LineNumber &&
                Quantity == other.Quantity && ExtendedPrice == other.ExtendedPrice && Discount == other.Discount && Tax == other.Tax &&
                ReturnFlag == other.ReturnFlag && LineStatus == other.LineStatus && ShipDate == other.ShipDate && CommitDate == other.CommitDate &&
                ReceiptDate == other.ReceiptDate && object.Equals(ShipInstruct, other.ShipInstruct) && object.Equals(ShipMode, other.ShipMode) &&
                object.Equals(Comment, other.Comment);
        }

        #endregion
    }
}
