// $Id$
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class PipelinePrepartitionedPushTaskRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
    {
        private readonly IPrepartitionedPushTask<TRecord, TPipelinedTaskOutput> _task;
        private readonly IPartitioner<TRecord> _partitioner;
        private PrepartitionedRecordWriter<TPipelinedTaskOutput> _output;

        public PipelinePrepartitionedPushTaskRecordWriter(IPrepartitionedPushTask<TRecord, TPipelinedTaskOutput> task, RecordWriter<TPipelinedTaskOutput> output, IPartitioner<TRecord> partitioner)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( output == null )
                throw new ArgumentNullException("output");
            if( partitioner == null )
                throw new ArgumentNullException("partitioner");

            _task = task;
            _output = new PrepartitionedRecordWriter<TPipelinedTaskOutput>(output);
            _partitioner = partitioner;
        }

        public void Finish()
        {
            _task.Finish(_output);
        }

        protected override void WriteRecordInternal(TRecord record)
        {
            _task.ProcessRecord(record, _partitioner.GetPartition(record), _output);
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
