// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    public sealed partial class JobBuilder
    {
        /// <summary>
        /// Sorts the specified input.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="comparerType">The type of <see cref="IComparer{T}"/> to use for this operation, or <see langword="null"/> to use the default comparer.</param>
        /// <returns>A <see cref="SortOperation"/> instance that can be used to further customize the operation.</returns>
        public SortOperation Sort(IOperationInput input, Type comparerType = null)
        {
            CheckIfInputBelongsToJobBuilder(input);
            return new SortOperation(this, input, comparerType, false);
        }

        /// <summary>
        /// Sorts the specified input by using a file channel with an output type of <see cref="Channels.FileChannelOutputType.SortSpill"/>.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <returns>A <see cref="SortOperation"/> instance that can be used to further customize the operation.</returns>
        public SortOperation SpillSort(IOperationInput input)
        {
            CheckIfInputBelongsToJobBuilder(input);
            return new SortOperation(this, input, null, true);
        }
    }
}
