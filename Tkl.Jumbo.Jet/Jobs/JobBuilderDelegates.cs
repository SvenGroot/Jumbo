﻿// $Id$
//
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
    /// <param name="context">The context for the task.</param>
    public delegate void TaskFunction<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output, TaskContext context);

    /// <summary>
    /// Delegate for push tasks.
    /// </summary>
    /// <typeparam name="TInput">The type of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <param name="record">The input record to process.</param>
    /// <param name="output">The record writer collecting the output records.</param>
    /// <param name="context">The configuration for the task.</param>
    public delegate void PushTaskFunction<TInput, TOutput>(TInput record, RecordWriter<TOutput> output, TaskContext context);

    /// <summary>
    /// Delegate for accumulator tasks
    /// </summary>
    /// <typeparam name="TKey">The type of the keys.</typeparam>
    /// <typeparam name="TValue">The type of the values.</typeparam>
    /// <param name="key">The key of the record.</param>
    /// <param name="currentValue">The value associated with the key in the accumulator that must be updated.</param>
    /// <param name="newValue">The new value associated with the key.</param>
    /// <returns>The new value.</returns>
    /// <remarks>
    /// <para>
    ///   If <typeparamref name="TValue"/> is a mutable reference type, it is recommended for performance reasons to update the
    ///   existing instance passed in <paramref name="currentValue"/> and then return that same instance from this method.
    /// </para>
    /// </remarks>
    public delegate TValue AccumulatorFunction<TKey, TValue>(TKey key, TValue currentValue, TValue newValue)
        where TKey : IComparable<TKey>;

    /// <summary>
    /// Delegate for tasks with no input.
    /// </summary>
    /// <typeparam name="T">The type of the output records.</typeparam>
    /// <param name="output">The record writer collecting the output records.</param>
    /// <param name="context">The configuration for the task.</param>
    public delegate void OutputOnlyTaskFunction<T>(RecordWriter<T> output, TaskContext context);

    /// <summary>
    /// Delegate for map tasks.
    /// </summary>
    /// <typeparam name="TInput">The type of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <param name="record">The input record to process.</param>
    /// <param name="output">The record writer collecting the output records.</param>
    /// <param name="context">The <see cref="TaskContext"/> for the task.</param>
    public delegate void MapFunction<TInput, TOutput>(TInput record, RecordWriter<TOutput> output, TaskContext context);

    /// <summary>
    /// Delegate for reduce tasks.
    /// </summary>
    /// <typeparam name="TKey">The type of the key of the input records.</typeparam>
    /// <typeparam name="TValue">The type of the value of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <param name="key">The key of the current set of records.</param>
    /// <param name="values">The values for the current <paramref name="key"/>.</param>
    /// <param name="output">The record writer collecting the output records.</param>
    /// <param name="context">The <see cref="TaskContext"/> for the task.</param>
    public delegate void ReduceFunction<TKey, TValue, TOutput>(TKey key, IEnumerable<TValue> values, RecordWriter<TOutput> output, TaskContext context);

#pragma warning restore 1587
#pragma warning restore 1591

}
