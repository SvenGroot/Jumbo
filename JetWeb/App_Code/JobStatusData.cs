// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Ookii.Jumbo.Jet;
using System.Collections.ObjectModel;
using Ookii.Jumbo;

/// <summary>
/// Summary description for JobStatusData
/// </summary>
public class JobStatusData
{
    private Collection<TaskStatusData> _failedTaskAttempts = new Collection<TaskStatusData>();
    private readonly Collection<StageStatusData> _stages = new Collection<StageStatusData>();

    public JobStatusData()
    {
    }

    public JobStatusData(JobStatus job)
    {
        JobId = job.JobId;
        JobName = job.JobName;
        RunningTaskCount = job.RunningTaskCount;
        UnscheduledTaskCount = job.UnscheduledTaskCount;
        FinishedTaskCount = job.FinishedTaskCount;
        NonDataLocalTaskCount = job.NonDataLocalTaskCount;
        StartTime = job.StartTime;
        EndTime = job.EndTime;
        IsFinished = job.IsFinished;

        foreach( StageStatus stage in job.Stages )
        {
            _stages.Add(new StageStatusData(stage));
        }
        foreach( TaskStatus failedTask in job.FailedTaskAttempts )
        {
            _failedTaskAttempts.Add(new TaskStatusData(failedTask));
        }
    }

    /// <summary>
    /// Gets or sets the ID of the job whose status this object represents.
    /// </summary>
    public Guid JobId { get; set; }

    /// <summary>
    /// Gets or sets the display name of the job.
    /// </summary>
    public string JobName { get; set; }

    /// <summary>
    /// Gets the stages of this job.
    /// </summary>
    public Collection<StageStatusData> Stages
    {
        get { return _stages; }
    }

    /// <summary>
    /// Gets the task attempts that failed.
    /// </summary>
    public Collection<TaskStatusData> FailedTaskAttempts
    {
        get { return _failedTaskAttempts; }
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
}
