using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface for task classes.
    /// </summary>
    public interface ITask<TInput, TOutput>
        where TInput : IWritable, new()
        where TOutput : IWritable
    {
        /// <summary>
        /// Runs the task.
        /// </summary>
        void Run(RecordReader<TInput> input, RecordWriter<TOutput> output);
    }
}
