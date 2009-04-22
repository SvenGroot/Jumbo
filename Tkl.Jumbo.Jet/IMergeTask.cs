using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface for tasks that merge the output of several tasks.
    /// </summary>
    /// <typeparam name="TInput">The input type of the task.</typeparam>
    /// <typeparam name="TOutput">The output type of the task.</typeparam>
    /// <remarks>
    /// <para>
    ///   Although you can use the <see cref="IPullTask{TInput,TOutput}"/> and <see cref="IPushTask{TInput,TOutput}"/>
    ///   for a task that reads input from multiple sources, it is not possible to distinguish between the input from
    ///   the different tasks using those interfaces.
    /// </para>
    /// <para>
    ///   Tasks implementing the <see cref="IMergeTask{TInput,TOutput}"/> interface will get a different <see cref="RecordReader{T}"/>
    ///   for each input task, so they can merge in a custom fashion (e.g. merge sort).
    /// </para>
    /// </remarks>
    public interface IMergeTask<TInput, TOutput> : ITask<TInput, TOutput>
        where TInput : IWritable, new()
        where TOutput : IWritable, new()
    {
        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A list of <see cref="RecordReader{T}"/> instances from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        void Run(MergeTaskInput<TInput> input, RecordWriter<TOutput> output);
    }
}
