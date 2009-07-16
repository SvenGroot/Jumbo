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
        /// Gets the total number of inputs readers that this record reader will have.
        /// </summary>
        int TotalInputCount { get; }

        /// <summary>
        /// Gets the current number of inputs that have been added to the <see cref="MultiInputRecordReader{T}"/>.
        /// </summary>
        int CurrentInputCount { get; }

        /// <summary>
        /// Adds the specified record reader to the inputs to be read by this record reader.
        /// </summary>
        /// <param name="reader">The record reader to read from.</param>
        void AddInput(IRecordReader reader);

        /// <summary>
        /// Adds the specified input file to the inputs to be read by this record reader.
        /// </summary>
        /// <param name="recordReaderType">The type of the record reader to be created to read the input file. This type be derived from <see cref="RecordReader{T}"/> and have a constructor with the same 
        /// parameters as <see cref="BinaryRecordReader{T}(string,bool,bool,int,Tkl.Jumbo.CompressionType,long)"/>.</param>
        /// <param name="fileName">The file to read.</param>
        /// <param name="sourceName">A name used to identify the source of this input. Can be <see langword="null"/>.</param>
        /// <param name="uncompressedSize">The size of the file's data after decompression; only needed if <see cref="CompressionType"/> is not <see cref="Tkl.Jumbo.CompressionType.None"/>.</param>
        /// <param name="deleteFile"><see langword="true"/> to delete the file after reading finishes; otherwise, <see langword="false"/>.</param>
        void AddInput(Type recordReaderType, string fileName, string sourceName, long uncompressedSize, bool deleteFile);
    }
}
