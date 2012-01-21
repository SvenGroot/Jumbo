// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="RecordInput"/> that reads the input from an existing record reader.
    /// </summary>
    public sealed class ReaderRecordInput : RecordInput
    {
        private readonly bool _isMemoryBased;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReaderRecordInput"/> class.
        /// </summary>
        /// <param name="reader">The record reader for this input.</param>
        /// <param name="isMemoryBased">If set to <see langword="true"/>, the input is memory based; if <see langword="false" />, the input is read from a file.</param>
        public ReaderRecordInput(IRecordReader reader, bool isMemoryBased)
            : base(reader)
        {
            _isMemoryBased = isMemoryBased;
        }

        /// <summary>
        /// Gets a value indicating whether this input is read from memory.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this input is read from memory; <see langword="false"/> if it is read from a file.
        /// </value>
        public override bool IsMemoryBased
        {
            get { return _isMemoryBased; }
        }

        /// <summary>
        /// Creates the record reader for this input. This function is not used by the <see cref="ReaderRecordInput"/>
        /// </summary>
        /// <param name="multiInputReader">The multi input record reader that this <see cref="RecordInput"/> instance belongs to.</param>
        /// <returns>
        /// The record reader for this input.
        /// </returns>
        protected override IRecordReader CreateReader(IMultiInputRecordReader multiInputReader)
        {
            // Not called if the RecordInput.RecordInput(IRecordReader) constructor is used.
            throw new NotSupportedException();
        }
    }
}
