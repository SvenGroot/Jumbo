using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet.Samples.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;
using System.Runtime.InteropServices;

namespace Tkl.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for GenWords, which generates randomized textual data from a list of words.
    /// </summary>
    [Description("Generates input for the WordCount job.")]
    public class GenWords : BasicJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenWords));

        private int _sizePerTask;
        private readonly string _dictionaryDirectory;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenWords"/> class.
        /// </summary>
        /// <param name="outputPath">The directory on the DFS to write the generated records to.</param>
        /// <param name="dictionaryPath">The directory on the Jumbo DFS where the dictionary files are stored.</param>
        /// <param name="taskCount">The number of generator tasks to use.</param>
        /// <param name="sizePerTaskMB">The size, in megabytes, of the data to generate per task. Specify zero to use the DFS block size.</param>
        public GenWords([Description("The output directory on the Jumbo DFS where the generated data will be written.")] string outputPath,
                        [Description("The directory on the Jumbo DFS where the dictionary files are stored.")] string dictionaryPath, 
                        [Description("The number of generator tasks to use.")] int taskCount,
                        [Description("The size, in megabytes, of the data to generate per task. Specify zero to use the DFS block size."), Optional, DefaultParameterValue(0)] int sizePerTaskMB)
            : base(null, outputPath, 0, typeof(GenWordsTask), null, null, null, null, typeof(TextRecordWriter<StringWritable>), null, false)
        {
            if( dictionaryPath == null )
                throw new ArgumentNullException("dictionaryPath");
            if( taskCount <= 0 )
                throw new ArgumentOutOfRangeException("taskCount", "You must use at least one generator task.");
            if( sizePerTaskMB < 0 )
                throw new ArgumentOutOfRangeException("sizePerTaskMB", "The size of the data to generate must be zero or greater.");

            FirstStageTaskCount = taskCount;
            _sizePerTask = sizePerTaskMB * 1024 * 1024;
            _dictionaryDirectory = dictionaryPath;
        }

        /// <summary>
        /// Overrides <see cref="BasicJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            base.OnJobCreated(job, jobConfiguration);

            DfsClient client = new DfsClient(DfsConfiguration);
            if( client.NameServer.GetDirectoryInfo(_dictionaryDirectory) == null )
                throw new ArgumentException("The dictionary directory does not exist.", "dictionaryPath");

            if( _sizePerTask == 0 )
                _sizePerTask = client.NameServer.BlockSize;

            _log.InfoFormat("Generating {0}MB per task with {1} tasks, total {2}MB.", _sizePerTask / 1024 / 1024, FirstStageTaskCount, (_sizePerTask * FirstStageTaskCount) / 1024 / 1024);

            jobConfiguration.AddSetting(GenWordsTask.DictionaryDirectorySetting, _dictionaryDirectory);
            jobConfiguration.AddTypedSetting(GenWordsTask.SizePerTaskSetting, _sizePerTask);
        }
    }
}
