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
        /// Creates a <see cref="StreamRecordReader{T}"/> from which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="StreamRecordReader{T}"/> for the channel.</returns>
        RecordReader<T> CreateRecordReader<T>() where T : IWritable, new();
    }
}
