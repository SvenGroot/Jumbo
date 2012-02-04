using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents the output of an operation. Can either be a channel or DFS output.
    /// </summary>
    public interface IOperationOutput
    {
        /// <summary>
        /// Gets the type of the records that can be written to this output.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        Type RecordType { get; }

        /// <summary>
        /// Applies the output settings to the specified stage.
        /// </summary>
        /// <param name="stage">The stage.</param>
        /// <remarks>
        /// <para>
        ///   This does nothing for channels; it is only relevant for DFS output.
        /// </para>
        /// </remarks>
        void ApplyOutput(StageConfiguration stage);
    }
}
