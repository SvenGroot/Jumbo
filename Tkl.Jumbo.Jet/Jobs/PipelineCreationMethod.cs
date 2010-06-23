// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Indicates how a stage may be duplicated as a pipelined stage on its input channel's sending end.
    /// </summary>
    enum PipelineCreationMethod
    {
        /// <summary>
        /// Automatic creation of a pipeline stage is not allowed.
        /// </summary>
        None,
        /// <summary>
        /// Automatic creation of a pipeline stage is allowed, and internal partitioning should be used.
        /// </summary>
        PrePartitioned,
        /// <summary>
        /// Automatic creation of a pipeline stage is allowed, but internal partitioning should not be used.
        /// </summary>
        PostPartitioned
    }
}
