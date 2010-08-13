// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Indicates what part of a stage a setting applies to.
    /// </summary>
    /// <remarks>
    /// This influences how the <see cref="JobBuilder"/> will treat the setting when it creates automatic pipeline channels.
    /// </remarks>
    public enum StageSettingCategory
    {
        /// <summary>
        /// The setting is used by the task. If an additional pipelined stage is created, the setting is applied to both stages, unless the stage uses <see cref="Tkl.Jumbo.Jet.Tasks.EmptyTask{T}"/>, in which
        /// case the setting is not applied to that stage.
        /// </summary>
        Task,
        /// <summary>
        /// The setting is used by the partitioner. If an additional pipelined stage is created, the setting is applied only to the output stage.
        /// Settings of the parent stage with this category will be copied to the child stage.
        /// </summary>
        Partitioner,
        /// <summary>
        /// The setting is used by the input channel. If an additional pipelined stage is created, the setting is applied only to the output stage.
        /// </summary>
        InputChannel,
        /// <summary>
        /// The setting is used by the input record reader (usually the multi-input record reader). If an additional pipelined stage is created, the setting is applied only to the output stage.
        /// </summary>
        InputRecordReader,
        /// <summary>
        /// The setting is used by the output channel. If an additional pipelined stage is created, the setting is applied only to the output stage.
        /// Settings of the parent stage with this category will be moved to the child stage.
        /// </summary>
        OutputChannel,
    }
}
