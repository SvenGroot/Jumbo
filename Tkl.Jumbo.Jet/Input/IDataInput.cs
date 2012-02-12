// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Jet.Input
{
    /// <summary>
    /// Provides methods for defining input (other than a channel) to a stage.
    /// </summary>
    public interface IDataInput
    {
        /// <summary>
        /// Gets the inputs for each task.
        /// </summary>
        /// <value>
        /// A list of task inputs, or <see langword="null"/> if the job is not being constructed. The returned collection may be read-only.
        /// </value>
        IList<ITaskInput> TaskInputs { get; }

        /// <summary>
        /// Gets the type of the records of this input.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        Type RecordType { get; }

        /// <summary>
        /// Creates the record reader for the specified task.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="jetConfiguration">The Jumbo Jet configuration. May be <see langword="null"/>.</param>
        /// <param name="context">The task context. May be <see langword="null"/>.</param>
        /// <param name="input">The task input.</param>
        /// <returns>
        /// The record reader.
        /// </returns>
        IRecordReader CreateRecordReader(FileSystemClient fileSystem, JetConfiguration jetConfiguration, TaskContext context, ITaskInput input);
    }
}
