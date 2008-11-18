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
        /// Gets a value that indicates whether the input channel is ready to begin reading data.
        /// </summary>
        bool IsReady { get; }

        /// <summary>
        /// Waits until the input channel becomes ready.
        /// </summary>
        /// <param name="timeout">The maximum amount of time to wait, or <see cref="System.Threading.Timeout.Infinite"/> to wait
        /// indefinitely.</param>
        /// <returns><see langword="true"/> if the channel has become ready; otherwise, <see langword="false"/>.</returns>
        bool WaitUntilReady(int timeout);

        /// <summary>
        /// Creates a <see cref="StreamRecordReader{T}"/> from which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="StreamRecordReader{T}"/> for the channel.</returns>
        RecordReader<T> CreateRecordReader<T>() where T : IWritable, new();
    }
}
