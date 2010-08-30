// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Tkl.Jumbo.Jet;

/// <summary>
/// Summary description for TaskStatusData
/// </summary>
public class TaskStatusData
{
    public TaskStatusData()
    {
    }

	public TaskStatusData(TaskStatus task)
	{
        TaskId = task.TaskId;
        State = task.State;
        TaskServer = task.TaskServer.ToString();
        Attempts = task.Attempts;
        StartTime = task.StartTime;
        EndTime = task.EndTime;
        Progress = task.Progress;
	}

    /// <summary>
    /// Gets or sets the ID of this task.
    /// </summary>
    public string TaskId { get; set; }

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
    public string TaskServer { get; set; }

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
    /// Gets or sets the progress of the task.
    /// </summary>
    public float Progress { get; set; }
}