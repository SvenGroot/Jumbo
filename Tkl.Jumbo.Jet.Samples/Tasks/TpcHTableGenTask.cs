// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Samples.IO;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task to generate TPC-H table data.
    /// </summary>
    public class TpcHTableGenTask : Configurable, ITask<int, LineItem>
    {
        /// <summary>
        /// The name of the setting in the <see cref="JobConfiguration.JobSettings"/> that specifies the filename of the dbgen executable.
        /// </summary>
        public const string DbGenFileNameSetting = "DbGenFileName";
        /// <summary>
        /// The name of the setting in the <see cref="JobConfiguration.JobSettings"/> that specifies the scale factor of the database.
        /// </summary>
        public const string ScaleFactorSetting = "ScaleFactor";

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TpcHTableGenTask));
        private readonly LineItem _record = new LineItem();
        
        #region ITask<int,LineItem> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<int> input, RecordWriter<LineItem> output)
        {
            string dbGenFileName = TaskContext.JobConfiguration.GetSetting(DbGenFileNameSetting, "dbgen");
            string dbGenPath = Path.Combine(TaskContext.LocalJobDirectory, dbGenFileName);
            int scaleFactor = TaskContext.JobConfiguration.GetTypedSetting(ScaleFactorSetting, 1);

            RuntimeEnvironment.MarkFileAsExecutable(dbGenPath); // This is required for Unix.

            // TODO: other tables.
            _log.InfoFormat("Generating segment {0} out of a total of {1}; using scale factor {2}.", TaskContext.TaskId.TaskNumber, TaskContext.StageConfiguration.TaskCount, scaleFactor);
            string arguments = string.Format(System.Globalization.CultureInfo.InvariantCulture, "-D -T L -C {0} -S {1}", TaskContext.StageConfiguration.TaskCount, TaskContext.TaskId.TaskNumber);
            ProcessStartInfo startInfo = new ProcessStartInfo(dbGenPath, arguments)
            {
                WorkingDirectory = TaskContext.LocalJobDirectory,
                UseShellExecute = false,
                RedirectStandardError = true,
                RedirectStandardOutput = true
            };

            using( Process process = new Process() )
            {
                process.StartInfo = startInfo;
                process.ErrorDataReceived += new DataReceivedEventHandler(process_ErrorDataReceived);
                _log.InfoFormat("Starting executable '{0}' with arguments '{1}'", dbGenPath, arguments);
                process.Start();
                process.BeginErrorReadLine();
                int pid = process.Id;
                _log.InfoFormat("Process {0} started.", pid);

                StreamReader outputReader = process.StandardOutput;
                using( BinaryReader reader = new BinaryReader(outputReader.BaseStream) )
                {
                    try
                    {
                        while( true )
                        {
                            _record.Read(reader);
                            output.WriteRecord(_record);
                        }
                    }
                    catch( EndOfStreamException )
                    {
                        _log.InfoFormat("End of standard output reached.");
                    }
                }

                process.WaitForExit();
                _log.InfoFormat("Process {0} ended with code {1}.", pid, process.ExitCode);
            }
        }

        #endregion

        private void process_ErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            _log.InfoFormat("dbgen: {0}", e.Data);
        }
    }
}
