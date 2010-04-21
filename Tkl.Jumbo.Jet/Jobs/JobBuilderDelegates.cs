using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Jobs
{

  // Bug in Mono C# compiler gives XML comment warning about delegates
#pragma warning disable 1587
#pragma warning disable 1591

    /// <summary>
    /// Delegate for tasks.
    /// </summary>
    /// <typeparam name="TInput">The type of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <param name="input">The record reader providing the input records.</param>
    /// <param name="output">The record writer collecting the output records.</param>
    public delegate void TaskFunction<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output)
        where TInput : IWritable, new()
        where TOutput : IWritable, new();

    /// <summary>
    /// Delegate for tasks.
    /// </summary>
    /// <typeparam name="TInput">The type of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <param name="input">The record reader providing the input records.</param>
    /// <param name="output">The record writer collecting the output records.</param>
    /// <param name="configuration">The configuration for the task.</param>
    public delegate void TaskFunctionWithConfiguration<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output, TaskAttemptConfiguration configuration)
        where TInput : IWritable, new()
        where TOutput : IWritable, new();

    /// <summary>
    /// Delegate for accumulator tasks
    /// </summary>
    /// <typeparam name="TKey">The type of the keys.</typeparam>
    /// <typeparam name="TValue">The type of the values.</typeparam>
    /// <param name="key">The key of the record.</param>
    /// <param name="value">The value associated with the key in the accumulator that must be updated.</param>
    /// <param name="newValue">The new value associated with the key.</param>
    public delegate void AccumulatorFunction<TKey, TValue>(TKey key, TValue value, TValue newValue)
        where TKey : IWritable, IComparable<TKey>, new()
        where TValue : class, IWritable, new();

    /// <summary>
    /// Delegate for tasks with no input.
    /// </summary>
    /// <typeparam name="T">The type of the output records.</typeparam>
    /// <param name="output">The record writer collecting the output records.</param>
    public delegate void OutputOnlyTaskFunction<T>(RecordWriter<T> output)
        where T : IWritable, new();

    /// <summary>
    /// Delegate for tasks with no input.
    /// </summary>
    /// <typeparam name="T">The type of the output records.</typeparam>
    /// <param name="output">The record writer collecting the output records.</param>
    /// <param name="configuration">The configuration for the task.</param>
    public delegate void OutputOnlyTaskFunctionWithConfiguration<T>(RecordWriter<T> output, TaskAttemptConfiguration configuration)
        where T : IWritable, new();

#pragma warning restore 1587
#pragma warning restore 1591

}
