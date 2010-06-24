// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents the input to a stage in a job being built by the <see cref="JobBuilder"/> class, such as a channel or DFS input.
    /// </summary>
    public interface IStageInput
    {
        /// <summary>
        /// Gets the type of the records read from the input.
        /// </summary>
        /// <value>
        /// A <see cref="Type"/> instance for the type of the records, or <see langword="null"/> if the type hasn't been determined yet.
        /// </value>
        Type RecordType { get; }
    }
}
