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
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        /// <remarks>
        /// If the task has more than one input, the record reader will combine all inputs, usually by serializing them.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        RecordReader<T> CreateRecordReader<T>() where T : IWritable, new();

        /// <summary>
        /// Creates a separate <see cref="RecordReader{T}"/> for each input task of the channel.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="MergeTaskInput{T}"/> that provides access to a list of <see cref="RecordReader{T}"/> instances.</returns>
        /// <remarks>
        /// <para>
        ///   Implementers should use the <see cref="RecordReader{T}.SourceName"/> property to indicate which task each reader reads from.
        /// </para>
        /// <para>
        ///   This method is used to create the input for a <see cref="IMergeTask{TInput,TOutput}"/>.
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        MergeTaskInput<T> CreateMergeTaskInput<T>() where T : IWritable, new();
    }
}
