// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Tkl.Jumbo.Jet;
using System.Collections.ObjectModel;

/// <summary>
/// Summary description for StageStatusData
/// </summary>
public class StageStatusData
{
    private readonly Collection<TaskStatusData> _tasks = new Collection<TaskStatusData>();

    public StageStatusData()
    {
    }

	public StageStatusData(StageStatus stage)
	{
        foreach( TaskStatus task in stage.Tasks )
        {
            _tasks.Add(new TaskStatusData(task));
        }
	}

    public Collection<TaskStatusData> Tasks
    {
        get { return _tasks; }
    }
}