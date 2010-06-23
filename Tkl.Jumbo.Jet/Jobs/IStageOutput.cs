// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents the output from a stage in a job being built by the <see cref="OldJobBuilder"/> class, such as a channel or DFS output.
    /// </summary>
    public interface IStageOutput
    {
        /// <summary>
        /// Gets the type of the records written to the output.
        /// </summary>
        /// <value>
        /// A <see cref="Type"/> instance for the type of the records, or <see langword="null"/> if the type hasn't been determined yet.
        /// </value>
        Type RecordType { get; }
    }
}
