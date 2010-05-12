// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Used as intermediate type for the PFP growth job.
    /// </summary>
    public class Transaction : IWritable
    {
        private int[] _items;

        /// <summary>
        /// Gets or sets the items.
        /// </summary>
        /// <value>The items.</value>
        public int[] Items
        {
            get { return _items; }
            set { _items = value; }
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            if( _items == null )
                WritableUtility.Write7BitEncodedInt32(writer, 0);
            else
            {
                WritableUtility.Write7BitEncodedInt32(writer, _items.Length);
                foreach( int item in _items )
                    writer.Write(item);
            }
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            int length = WritableUtility.Read7BitEncodedInt32(reader);
            _items = new int[length];
            for( int x = 0; x < length; ++x )
                _items[x] = reader.ReadInt32();
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return "{ " + _items.ToDelimitedString() + " }";
        }
    }
}
