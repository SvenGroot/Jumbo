// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Specifies the connectivity of a channel
    /// </summary>
    public enum ChannelConnectivity
    {
        /// <summary>
        /// Each input should be connected to each output. Note this is done at the root level of a compound stage; if the
        /// input stage is a child stage, each output task will be connected to one child task of each root input task.
        /// In this case the number of tasks in the output stage must match the number of tasks in the child input stage.
        /// </summary>
        Full,
        /// <summary>
        /// One input should be connected to each output. The number of tasks in the input stage and in the output stage
        /// need to match, and if the input stage is a child stage, the number of tasks in the output stage needs to
        /// match the number of tasks in the input stage multiplied by the number of tasks in its parent stage(s).
        /// </summary>
        PointToPoint
    }
}
