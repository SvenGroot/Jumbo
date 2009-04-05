﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using Tkl.Jumbo.Jet;

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
    public JobStatus GetJobStatus(Guid jobId)
    {
        JetClient client = new JetClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);
        // Set the end time to the current time for jobs that are running so they get displayed correctly.
        foreach( TaskStatus task in job.Tasks )
        {
            if( task.State == TaskState.Running )
                task.EndTime = DateTime.UtcNow;
        }
        return job;
    }

}
