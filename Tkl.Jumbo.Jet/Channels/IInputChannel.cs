// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Interface for input channels for task communication.
    /// </summary>
    public interface IInputChannel
    {
        /// <summary>
        /// Gets the configuration of the input channel.
        /// </summary>
        /// <value>The configuration of the input channel.</value>
        ChannelConfiguration Configuration { get; }

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        /// <remarks>
        /// If the task has more than one input, the record reader will combine all inputs, usually by serializing them.
        /// </remarks>
        IRecordReader CreateRecordReader();

        /// <summary>
        /// Assigns additional partitions to this input channel.
        /// </summary>
        /// <param name="additionalPartitions">The additional partitions.</param>
        /// <remarks>
        /// <para>
        ///   Not all input channels need to support this.
        /// </para>
        /// <para>
        ///   This method will never be called if <see cref="ChannelConfiguration.PartitionsPerTask"/> is 1.
        /// </para>
        /// </remarks>
        void AssignAdditionalPartitions(IList<int> additionalPartitions);
    }
}
