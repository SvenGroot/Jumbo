// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Interface for streams that compress data.
    /// </summary>
    public interface ICompressor
    {
        /// <summary>
        /// When compressing, gets the number of compressed bytes written.
        /// </summary>
        long CompressedBytesWritten { get; }

        /// <summary>
        /// When compressing, gets the number of uncompressed bytes written.
        /// </summary>
        long UncompressedBytesWritten { get; }

        /// <summary>
        /// When decompressing, gets the number of compressed bytes read.
        /// </summary>
        long CompressedBytesRead { get; }

        /// <summary>
        /// When decompressing, gets the number of uncompressed bytes read.
        /// </summary>
        long UncompressedBytesRead { get; }

        /// <summary>
        /// Gets the length of the underlying compressed stream.
        /// </summary>
        long CompressedSize { get; }
    }
}
