// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Runtime.Serialization;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// A collection of frequent patterns for a specific item, using the item mapping.
    /// </summary>
    public class MappedFrequentPatternCollection : IWritable
    {
        private List<MappedFrequentPattern> _patterns = new List<MappedFrequentPattern>();

        /// <summary>
        /// Gets or sets the item ID of the item these patterns are for.
        /// </summary>
        public int ItemId { get; set; }

        /// <summary>
        /// Gets the patterns.
        /// </summary>
        /// <value>The patterns.</value>
        public IList<MappedFrequentPattern> Patterns
        {
            get { return _patterns; }
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            writer.Write(ItemId);
            WritableUtility.Write7BitEncodedInt32(writer, _patterns.Count);
            foreach( MappedFrequentPattern pattern in _patterns )
                pattern.Write(writer);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(System.IO.BinaryReader reader)
        {
            ItemId = reader.ReadInt32();
            int count = WritableUtility.Read7BitEncodedInt32(reader);
            if( _patterns == null )
                _patterns = new List<MappedFrequentPattern>(count);
            else
                _patterns.Clear();

            for( int x = 0; x < count; ++x )
            {
                MappedFrequentPattern pattern = (MappedFrequentPattern)FormatterServices.GetUninitializedObject(typeof(MappedFrequentPattern));
                pattern.Read(reader);
                _patterns.Add(pattern);
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "[{0}, {{ {1} }}]", ItemId, Patterns.ToDelimitedString());
        }
    }
}
