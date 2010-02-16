using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;

/// <summary>
/// Summary description for JobStatusService
/// </summary>
[WebService(Namespace = "http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/JobStatusService")]
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
    public JobStatus GetJobStatus(Guid jobId, DateTime lastUpdateTime)
    {
        JetClient client = new JetClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);
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
        return job;
    }

}

