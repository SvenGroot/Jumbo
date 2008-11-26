using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information for a specific job.
    /// </summary>
    [XmlRoot("Job", Namespace=JobConfiguration.XmlNamespace)]
    public class JobConfiguration
    {
        /// <summary>
        /// The XML namespace for the job configuration XML.
        /// </summary>
        public const string XmlNamespace = "http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/JobConfiguration";
        private static readonly XmlSerializer _serializer = new XmlSerializer(typeof(JobConfiguration));

        /// <summary>
        /// Gets or sets the file name of the assembly holding the task classes.
        /// </summary>
        public string AssemblyFileName { get; set; }

        /// <summary>
        /// Gets or sets a list of tasks that make up this job.
        /// </summary>
        public List<TaskConfiguration> Tasks { get; set; }

        /// <summary>
        /// Gets or sets a list of communication channels between the tasks.
        /// </summary>
        public List<Channels.ChannelConfiguration> Channels { get; set; }

        /// <summary>
        /// Gets the task with the specified ID.
        /// </summary>
        /// <param name="taskID">The ID of the task.</param>
        /// <returns>The <see cref="TaskConfiguration"/> for the task, or <see langword="null"/> if no task with that ID exists.</returns>
        public TaskConfiguration GetTask(string taskID)
        {
            return (from task in Tasks
                    where task.TaskID == taskID
                    select task).SingleOrDefault();
        }

        /// <summary>
        /// Saves the current instance as XML to the specified stream.
        /// </summary>
        /// <param name="stream">The stream to save to.</param>
        public void SaveXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            _serializer.Serialize(stream, this);
        }

        /// <summary>
        /// Gets the output channel configuration for a specific task.
        /// </summary>
        /// <param name="taskID">The task ID.</param>
        /// <returns>The channel configuration.</returns>
        public Channels.ChannelConfiguration GetOutputChannelForTask(string taskID)
        {
            return (from channel in Channels
                    where channel.InputTasks != null && channel.InputTasks.Contains(taskID)
                    select channel).SingleOrDefault();
        }


        /// <summary>
        /// Gets the input channel configuration for a specific task.
        /// </summary>
        /// <param name="taskID">The task ID.</param>
        /// <returns>The channel configuration.</returns>
        public Channels.ChannelConfiguration GetInputChannelForTask(string taskID)
        {
            return (from channel in Channels
                    where channel.OutputTasks != null && channel.OutputTasks.Contains(taskID)
                    select channel).SingleOrDefault();
        }

        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="stream">The stream containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        public static JobConfiguration LoadXml(System.IO.Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");
            return (JobConfiguration)_serializer.Deserialize(stream);
        }

        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="file">The path of the file containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        public static JobConfiguration LoadXml(string file)
        {
            if( file == null )
                throw new ArgumentNullException("file");
            using( System.IO.FileStream stream = System.IO.File.OpenRead(file) )
            {
                return LoadXml(stream);
            }
        }
    }
}
