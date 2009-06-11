using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Represents a record from the LINEITEM table of the TPC-H benchmark.
    /// </summary>
    public sealed class LineItem : Writable<LineItem>
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
        public UTF8StringWritable ShipInstruct { get; set; }

        /// <summary>
        /// L_SHIPMODE, fixed text, size 10
        /// </summary>
        public UTF8StringWritable ShipMode { get; set; }

        /// <summary>
        /// L_COMMENT, variable text, size 44
        /// </summary>
        public UTF8StringWritable Comment { get; set; }

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
            Quantity = (long)(Convert.ToDecimal(fields[4], System.Globalization.CultureInfo.InvariantCulture) * 100);
            ExtendedPrice = (long)(Convert.ToDecimal(fields[5], System.Globalization.CultureInfo.InvariantCulture) * 100);
            Discount = (long)(Convert.ToDecimal(fields[6], System.Globalization.CultureInfo.InvariantCulture) * 100);
            Tax = (long)(Convert.ToDecimal(fields[7], System.Globalization.CultureInfo.InvariantCulture) * 100);
            ReturnFlag = fields[8][0];
            LineStatus = fields[9][0];
            ShipDate = DateTime.ParseExact(fields[10], dateFormat, System.Globalization.CultureInfo.InvariantCulture);
            CommitDate = DateTime.ParseExact(fields[11], dateFormat, System.Globalization.CultureInfo.InvariantCulture);
            ReceiptDate = DateTime.ParseExact(fields[12], dateFormat, System.Globalization.CultureInfo.InvariantCulture);
            if( ShipInstruct == null )
                ShipInstruct = new UTF8StringWritable(fields[13]);
            else
                ShipInstruct.Set(fields[13]);
            if( ShipMode == null )
                ShipMode = new UTF8StringWritable(fields[14]);
            else
                ShipMode.Set(fields[14]);
            if( Comment == null )
                Comment = new UTF8StringWritable(fields[15]);
            else
                Comment.Set(fields[15]);
        }
    }
}
