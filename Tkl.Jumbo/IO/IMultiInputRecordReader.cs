using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Interface for record readers that combine the input of multiple record readers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record readers must inherit from <see cref="MultiInputRecordReader{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IMultiInputRecordReader : IRecordReader
    {
        /// <summary>
        /// Adds the specified record reader to the inputs to be read by this record reader.
        /// </summary>
        /// <param name="reader">The record reader to read from.</param>
        void AddInput(IRecordReader reader);
    }
}
