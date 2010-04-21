﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// A task that does nothing, but simply forwards the records to the output unmodified.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// This task is useful if you immediately want to partition your input without processing it first.
    /// </remarks>
    [AllowRecordReuse(PassThrough=true)]
    public class EmptyTask<T> : IPullTask<T, T>
        where T : IWritable, new()
    {
        #region IPullTask<T,T> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">The input for the task.</param>
        /// <param name="output">The output for the task.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public void Run(RecordReader<T> input, RecordWriter<T> output)
        {
            foreach( T record in input.EnumerateRecords() )
            {
                output.WriteRecord(record);
            }
        }

        #endregion
    }
}
