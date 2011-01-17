// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Jobs;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Task that invokes a serialied delegate from the stage settings to do the processing. This class is for internal
    /// Jumbo usage and should not be used by your code.
    /// </summary>
    /// <typeparam name="TInput">The type of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    public sealed class DelegatePullTask<TInput, TOutput> : Configurable, IPullTask<TInput, TOutput>
    {
        private TaskFunction<TInput, TOutput> _taskFunction;

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<TInput> input, RecordWriter<TOutput> output)
        {
            if( _taskFunction == null )
                throw new InvalidOperationException("No delegate specified.");
            _taskFunction(input, output, TaskContext);
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            _taskFunction = (TaskFunction<TInput, TOutput>)JobBuilder.DeserializeDelegate(TaskContext);
        }
    }
}
