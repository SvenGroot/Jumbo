using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about a task.
    /// </summary>
    [Serializable]
    public class TaskStatus
    {
        /// <summary>
        /// Gets or sets the ID of this task.
        /// </summary>
        public string TaskID { get; set; }

        /// <summary>
        /// Gets or sets the current state of the task.
        /// </summary>
        public TaskState State { get; set; }

        /// <summary>
        /// Gets or sets the task server that a job is assigned to.
        /// </summary>
        /// <remarks>
        /// If there has been more than one attempt, this information only applies to the current attempt.
        /// </remarks>
        public ServerAddress TaskServer { get; set; }

        /// <summary>
        /// Gets or sets the number of times this task has been attempted.
        /// </summary>
        public int Attempts { get; set; }

        /// <summary>
        /// Gets or sets the UTC start time of the task.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the UTC end time of the task.
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets the duration of the task.
        /// </summary>
        public TimeSpan Duration
        {
            get
            {
                return EndTime - StartTime;
            }
        }

        /// <summary>
        /// The amount of time after the start of the job that this task started.
        /// </summary>
        public TimeSpan StartOffset { get; set; }

        /// <summary>
        /// Gets or sets the instance ID of the task host that executed the task.
        /// </summary>
        /// <remarks>
        /// This value can be used to track the precise execution sequence of tasks on a particular task server.
        /// </remarks>
        public int ExecutionInstanceId { get; set; }

        /// <summary>
        /// Gets an XML element containing the task status.
        /// </summary>
        /// <returns>An <see cref="XElement"/> containing the task status.</returns>
        public XElement ToXml()
        {
            return new XElement("Task",
                new XAttribute("id", TaskID),
                new XAttribute("state", State.ToString()),
                new XAttribute("server", TaskServer.ToString()),
                new XAttribute("attempts", Attempts.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("startTime", StartTime.ToString(JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("endTime", EndTime.ToString(JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("duration", Duration.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("executionInstance", ExecutionInstanceId.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        }

        /// <summary>
        /// Creates a <see cref="TaskStatus"/> instance from an XML element.
        /// </summary>
        /// <param name="task">The XML element containing the task status.</param>
        /// <param name="job">The job that this task belongs to.</param>
        /// <returns>A new instance of the <see cref="TaskStatus"/> class with the information from the XML document.</returns>
        public static TaskStatus FromXml(XElement task, JobStatus job)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( job == null )
                throw new ArgumentNullException("job");

            if( task.Name != "Task" )
                throw new ArgumentException("Invalid task element.", "task");

            TaskStatus status = new TaskStatus()
            {
                TaskID = task.Attribute("id").Value,
                State = (TaskState)Enum.Parse(typeof(TaskState), task.Attribute("state").Value),
                TaskServer = new ServerAddress(task.Attribute("server").Value),
                Attempts = (int)task.Attribute("attempts"),
                StartTime = DateTime.ParseExact(task.Attribute("startTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                EndTime = DateTime.ParseExact(task.Attribute("endTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                ExecutionInstanceId = (int)task.Attribute("executionInstance"),
            };
            status.StartOffset = status.StartTime - job.StartTime;
            return status;
        }
    }
}
