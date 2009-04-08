using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// The type of a communication channel between two tasks.
    /// </summary>
    public enum ChannelType
    {
        /// <summary>
        /// The input task writes a file to disk, which the output task then downloads and reads from.
        /// </summary>
        File,
        /// <summary>
        /// The input task's output is directly pipelined to the output task.
        /// </summary>
        /// <remarks>
        /// This requires the output task to use <see cref="IPushTask{TInput,TOutput}"/>. Tasks connected by
        /// this channel type are treated as a single entity from the scheduler's point of view because they
        /// are executed in the same process.
        /// </remarks>
        Pipeline
    }
}
