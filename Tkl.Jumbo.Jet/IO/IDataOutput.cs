// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Jet.IO
{
    /// <summary>
    /// Provides method for defining data output (other than a channel) for a stage.
    /// </summary>
    public interface IDataOutput
    {
        /// <summary>
        /// Gets the type of the records used for this output.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        Type RecordType { get; }

        /// <summary>
        /// Creates the output for the specified partition.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="jetConfiguration">The Jumbo Jet configuration. May be <see langword="null"/>.</param>
        /// <param name="context">The task context.</param>
        /// <param name="partitionNumber">The partition number for this output.</param>
        /// <returns>
        /// The record writer.
        /// </returns>
        /// <remarks>
        /// Don't assume you have any state when this method is called. Instead, read any state necessary using the specified <paramref name="context"/>.
        /// </remarks>
        IOutputCommitter CreateOutput(FileSystemClient fileSystem, JetConfiguration jetConfiguration, TaskContext context, int partitionNumber);

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
