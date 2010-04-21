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
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        /// <remarks>
        /// If the task has more than one input, the record reader will combine all inputs, usually by serializing them.
        /// </remarks>
        IRecordReader CreateRecordReader();
    }
}
