// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.Jet.Jobs;

namespace Tkl.Jumbo.Jet.IO
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

        /// <summary>
        /// Notifies the data input that it has been added to a stage.
        /// </summary>
        /// <param name="stage">The stage configuration of the stage.</param>
        /// <remarks>
        /// <para>
        ///   Implement this method if you want to add any setting to the stage. Keep in mind that the stage may still be under construction, so not all its
        ///   properties may have their final values yet.
        /// </para>
        /// </remarks>
        void NotifyAddedToStage(StageConfiguration stage);
    }
}
