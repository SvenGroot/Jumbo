// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Interface for record readers that combine the input of multiple record readers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record readers must inherit from <see cref="MultiInputRecordReader{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IMultiInputRecordReader : IRecordReader
    {
        /// <summary>
        /// Event that is raised if the value of the <see cref="CurrentPartition"/> property changes.
        /// </summary>
        event EventHandler CurrentPartitionChanged;

        /// <summary>
        /// Gets the total number of inputs readers that this record reader will have.
        /// </summary>
        int TotalInputCount { get; }

        /// <summary>
        /// Gets the current number of inputs that have been added to the <see cref="MultiInputRecordReader{T}"/>.
        /// </summary>
        int CurrentInputCount { get; }

        /// <summary>
        /// Gets a value that indicates that this record reader is allowed to reuse record instances.
        /// </summary>
        bool AllowRecordReuse { get; }

        /// <summary>
        /// Gets the buffer size to use to read input files.
        /// </summary>
        int BufferSize { get; }

        /// <summary>
        /// Gets the type of compression to use to read input files.
        /// </summary>
        CompressionType CompressionType { get; }

        /// <summary>
        /// Gets all partitions that this reader currently has data for.
        /// </summary>
        IList<int> Partitions { get; }

        /// <summary>
        /// Gets or sets the partition that calls to <see cref="RecordReader{T}.ReadRecord"/> should return records for.
        /// </summary>
        int CurrentPartition { get; set; }

        /// <summary>
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input.</param>
        /// <remarks>
        /// Which partitions a multi input record reader is responsible for is specified when that reader is created.
        /// All calls to <see cref="AddInput"/> must specify those exact same partitions, sorted by the partition number.
        /// </remarks>
        void AddInput(IList<RecordInput> partitions);
    }
}
