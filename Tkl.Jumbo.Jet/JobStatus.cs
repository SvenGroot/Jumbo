using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about the currently running 
    /// </summary>
    [Serializable]
    public class JobStatus
    {
        private ExtendedCollection<TaskStatus> _tasks = new ExtendedCollection<TaskStatus>();
        private ExtendedCollection<TaskStatus> _failedTaskAttempts = new ExtendedCollection<TaskStatus>();

        internal const string DatePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

        /// <summary>
        /// Gets or sets the ID of the job whose status this object represents.
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the display name of the job.
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// Gets the tasks of this job.
        /// </summary>
        public Collection<TaskStatus> Tasks
        {
            get { return _tasks; }
        }

        /// <summary>
        /// Gets the task attempts that failed.
        /// </summary>
        public Collection<TaskStatus> FailedTaskAttempts
        {
            get { return _failedTaskAttempts; }
        }

        /// <summary>
        /// Gets or sets the total number of tasks in the 
        /// </summary>
        public int TaskCount
        {
            get { return Tasks.Count; }
        }

        /// <summary>
        /// Gets or sets the number of tasks currently running.
        /// </summary>
        public int RunningTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that has not yet been scheduled.
        /// </summary>
        public int UnscheduledTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that have finished.
        /// </summary>
        /// <remarks>
        /// This includes tasks that encountered an error.
        /// </remarks>
        public int FinishedTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that were not scheduled data local.
        /// </summary>
        /// <remarks>
        /// This only includes DFS input tasks; tasks that do not read from the DFS are never data local, and are not counted here.
        /// </remarks>
        public int NonDataLocalTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the UTC start time of the 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the UTC end time of the 
        /// </summary>
        /// <remarks>
        /// This property is not valid until the job is finished.
        /// </remarks>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the job has finished.
        /// </summary>
        public bool IsFinished { get; set; }

        /// <summary>
        /// Gets the number of task attempts that failed.
        /// </summary>
        [XmlIgnore]
        public int ErrorTaskCount
        {
            get
            {
                return FailedTaskAttempts == null ? 0 : FailedTaskAttempts.Count;
            }
        }

        /// <summary>
        /// Gets the total progress of the job, between 0 and 1.
        /// </summary>
        [XmlIgnore]
        public float Progress
        {
            get
            {
                return (from task in Tasks
                        select task.Progress).Average();
            }
        }

        /// <summary>
        /// Gets a value that indicates whether the job has finished successfully.
        /// </summary>
        public bool IsSuccessful
        {
            get { return FinishedTaskCount == TaskCount; }
        }

        /// <summary>
        /// Gets a string representatino of this <see cref="JobStatus"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="JobStatus"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{6:0.0}%, tasks: {0}, running: {1}, pending {2}, finished: {3}, errors: {4}, not local: {5}", TaskCount, RunningTaskCount, UnscheduledTaskCount, FinishedTaskCount, ErrorTaskCount, NonDataLocalTaskCount, Progress * 100);
        }

        /// <summary>
        /// Gets an XML document containing the job status.
        /// </summary>
        /// <returns>An <see cref="XDocument"/> containing the job status.</returns>
        public XDocument ToXml()
        {
            return new XDocument(new XDeclaration("1.0", "utf-8", null),
                new XElement("Job",
                    new XAttribute("id", JobId.ToString()),
                    new XElement("JobInfo",
                        new XAttribute("startTime", StartTime.ToString(DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("endTime", EndTime.ToString(DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("duration", (EndTime - StartTime).TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("tasks", TaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("finishedTasks", FinishedTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("errors", ErrorTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("nonDataLocalTasks", NonDataLocalTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture))),
                    new XElement("Tasks",
                        from task in Tasks
                        select task.ToXml()),
                    ErrorTaskCount == 0 ? 
                        null : 
                        new XElement("FailedTaskAttempts",
                            from task in FailedTaskAttempts
                            select task.ToXml())));
        }

        /// <summary>
        /// Creates a <see cref="JobStatus"/> instance from an XML element.
        /// </summary>
        /// <param name="job">The XML element containing the job status.</param>
        /// <returns>A new instance of the <see cref="JobStatus"/> class with the information from the XML document.</returns>
        public static JobStatus FromXml(XElement job)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            if( job.Name != "Job" )
                throw new ArgumentException("Invalid job element.", "job");

            XElement jobInfo = job.Element("JobInfo");
            JobStatus jobStatus = new JobStatus()
            {
                JobId = new Guid(job.Attribute("id").Value),
                StartTime = DateTime.ParseExact(jobInfo.Attribute("startTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                EndTime = DateTime.ParseExact(jobInfo.Attribute("endTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                FinishedTaskCount = (int)jobInfo.Attribute("finishedTasks"),
                NonDataLocalTaskCount = (int)jobInfo.Attribute("nonDataLocalTasks"),
            };
            jobStatus.Tasks.AddRange(from task in job.Element("Tasks").Elements("Task")
                                     select TaskStatus.FromXml(task, jobStatus));
            if( job.Element("FailedTaskAttempts") != null )
            {
                jobStatus.FailedTaskAttempts.AddRange(from task in job.Element("FailedTaskAttempts").Elements("Task")
                                                      select TaskStatus.FromXml(task, jobStatus));
            }
            return jobStatus;

        }
    }
}
