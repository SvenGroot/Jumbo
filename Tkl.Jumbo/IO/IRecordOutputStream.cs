// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Interface for streams that offer special handling of records.
    /// </summary>
    public interface IRecordOutputStream
    {
        /// <summary>
        /// Gets the options applied to records in the stream.
        /// </summary>
        /// <value>One or more of the <see cref="RecordStreamOptions"/> values.</value>
        RecordStreamOptions RecordOptions { get; }

        /// <summary>
        /// Indicates that the current position of the stream is a record boundary.
        /// </summary>
        void MarkRecord();
    }
}
