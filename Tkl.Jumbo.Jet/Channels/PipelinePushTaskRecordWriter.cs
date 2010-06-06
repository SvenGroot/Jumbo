// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class PipelinePushTaskRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
    {
        private IPushTask<TRecord, TPipelinedTaskOutput> _task;
        private RecordWriter<TPipelinedTaskOutput> _output;

        public PipelinePushTaskRecordWriter(IPushTask<TRecord, TPipelinedTaskOutput> task, RecordWriter<TPipelinedTaskOutput> output)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( output == null )
                throw new ArgumentNullException("output");

            _task = task;
            _output = output;
        }

        protected override void WriteRecordInternal(TRecord record)
        {
            if( _task == null )
                throw new ObjectDisposedException(typeof(PipelinePushTaskRecordWriter<TRecord, TPipelinedTaskOutput>).FullName);
            _task.ProcessRecord(record, _output);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( disposing )
                {
                    if( _output != null )
                    {
                        _output.Dispose();
                        _output = null;
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }

}
