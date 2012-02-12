// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    public sealed partial class JobBuilder
    {
        /// <summary>
        /// Generates records using a task that takes no input.
        /// </summary>
        /// <param name="taskCount">The number of tasks in the stage.</param>
        /// <param name="taskType">Type of the task. May be a generic type definition with a single type parameter.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <note>
        ///   The stage created by this method will have no input, so the input record reader for the task will be <see langword="null"/>.
        /// </note>
        /// <para>
        ///   You can use the <see cref="Tasks.NoInputTask{T}"/> class as a base class for tasks used with this method, although this is not a requirement.
        /// </para>
        /// </remarks>
        public StageOperation Generate(int taskCount, Type taskType)
        {
            return new StageOperation(this, taskCount, taskType);
        }

        /// <summary>
        /// Generates records using a task that takes no input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="taskCount">The task count.</param>
        /// <param name="generator">The generator function.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class implementing <see cref="ITask{TInput, TOutput}"/> which calls the target method of the <paramref name="generator"/> delegate
        ///   from the <see cref="ITask{TInput, TOutput}.Run"/> method.
        /// </para>
        /// <note>
        ///   The task method will be called from a completely different process than the one that is using <see cref="JobBuilder"/>, so it should not really
        ///   on any external state.
        /// </note>
        /// <para>
        ///   If the target method is a <see langword="public" /> <see langword="static"/> method, it will be called directly by the generated task class. Otherwise, the supplied
        ///   delegate will be serialized to the task settings and used to call the method. If the target method is an instance method, the instance it belongs to will be
        ///   serialized as well (this class must have the <see cref="SerializableAttribute"/> attribute).
        /// </para>
        /// </remarks>
        public StageOperation Generate<T>(int taskCount, Action<RecordWriter<T>, TaskContext> generator)
        {
            return GenerateCore<T>(taskCount, generator);
        }

        /// <summary>
        /// Generates records using a task that takes no input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="taskCount">The task count.</param>
        /// <param name="generator">The generator function.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class implementing <see cref="ITask{TInput, TOutput}"/> which calls the target method of the <paramref name="generator"/> delegate
        ///   from the <see cref="ITask{TInput, TOutput}.Run"/> method.
        /// </para>
        /// <note>
        ///   The task method will be called from a completely different process than the one that is using <see cref="JobBuilder"/>, so it should not really
        ///   on any external state.
        /// </note>
        /// <para>
        ///   If the target method is a <see langword="public" /> <see langword="static"/> method, it will be called directly by the generated task class. Otherwise, the supplied
        ///   delegate will be serialized to the task settings and used to call the method. If the target method is an instance method, the instance it belongs to will be
        ///   serialized as well (this class must have the <see cref="SerializableAttribute"/> attribute).
        /// </para>
        /// </remarks>
        public StageOperation Generate<T>(int taskCount, Action<RecordWriter<T>> generator)
        {
            return GenerateCore<T>(taskCount, generator);
        }
        
        private StageOperation GenerateCore<T>(int taskCount, Delegate generator)
        {
            if( generator == null )
                throw new ArgumentNullException("generator");

            // Record reuse is irrelevant for a task with no input.
            Type taskType = _taskBuilder.CreateDynamicTask(typeof(ITask<int, T>).GetMethod("Run"), generator, 1, RecordReuseMode.Default);

            StageOperation result = new StageOperation(this, taskCount, taskType);
            AddAssemblyAndSerializeDelegateIfNeeded(generator, result);
            return result;
        }
    }
}
