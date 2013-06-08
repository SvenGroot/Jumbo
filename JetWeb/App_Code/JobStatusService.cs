// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo;

/// <summary>
/// Summary description for JobStatusService
/// </summary>
[WebService(Namespace = "http://www.ookii.org/schema/Jumbo/JobStatusService")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
// To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
// [System.Web.Script.Services.ScriptService]
public class JobStatusService : System.Web.Services.WebService
{

    public JobStatusService()
    {

        //Uncomment the following line if using designed components 
        //InitializeComponent(); 
    }

    [WebMethod]
    public JobStatusData GetJobStatus(Guid jobId, DateTime lastUpdateTime, bool archived)
    {
        JetClient client = new JetClient();
        JobStatus job;
        if( archived )
            job = client.JobServer.GetArchivedJobStatus(jobId);
        else
            job = client.JobServer.GetJobStatus(jobId);
        // Set the end time to the current time for jobs that are running so they get displayed correctly.
        foreach( StageStatus stage in job.Stages )
        {
            List<TaskStatus> tasks = new List<TaskStatus>();
            foreach( TaskStatus task in stage.Tasks )
            {
                if( task.State == TaskState.Running )
                    task.EndTime = DateTime.UtcNow;

                if( task.State != TaskState.Created && (task.State != TaskState.Finished || task.EndTime >= lastUpdateTime) )
                    tasks.Add(task);
            }
            stage.Tasks.Clear();
            stage.Tasks.AddRange(tasks);
        }
        return new JobStatusData(job);
    }

}

