using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface for task classes.
    /// </summary>
    public interface ITask<TInput, TOutput>
    {
        /// <summary>
        /// Runs the task.
        /// </summary>
        void Run(RecordReader<TInput> input, RecordWriter<TOutput> output);
    }
}
