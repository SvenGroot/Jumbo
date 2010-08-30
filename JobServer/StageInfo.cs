// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Collections.ObjectModel;
using System.Threading;

namespace JobServerApplication
{
    sealed class StageInfo
    {
        private readonly List<TaskInfo> _tasks = new List<TaskInfo>();
        private readonly StageConfiguration _configuration;
        private List<StageInfo> _softDependentStages;
        private List<StageInfo> _hardDependentStages;
        private int _remainingTasks;
        private int _remainingSchedulingDependencies;

        public StageInfo(JobInfo job, StageConfiguration configuration)
        {
            _configuration = configuration;
            _remainingTasks = configuration.TaskCount;

            if( job != null )
            {
                IEnumerable<StageConfiguration> dependencies = job.Configuration.GetDependenciesForStage(configuration.StageId, false);
                foreach( StageConfiguration dependency in dependencies )
                {
                    StageInfo stage = job.GetStage(dependency.Root.StageId);
                    // We need to be notified if the dependency is finished if it is a hard dependency, or if it isn't ready for scheduling itself.
                    if( !(dependency.OutputChannel != null && dependency.OutputChannel.OutputStage == configuration.StageId) )
                    {
                        ++_remainingSchedulingDependencies;
                        if( stage._hardDependentStages == null )
                            stage._hardDependentStages = new List<StageInfo>();
                        stage._hardDependentStages.Add(this);
                    }
                    else if( !stage.IsReadyForScheduling )
                    {
                        ++_remainingSchedulingDependencies;
                        if( stage._softDependentStages == null )
                            stage._softDependentStages = new List<StageInfo>();
                        stage._softDependentStages.Add(this);
                    }
                }
            }
        }

        public string StageId
        {
            get { return _configuration.StageId; }
        }

        public StageConfiguration Configuration
        {
            get { return _configuration; }
        }

        public List<TaskInfo> Tasks
        {
            get { return _tasks; }
        }

        public bool IsReadyForScheduling
        {
            get { return _remainingSchedulingDependencies == 0; }
        }

        public void NotifyTaskFinished()
        {
            if( Interlocked.Decrement(ref _remainingTasks) == 0 )
                NotifyHardDependentStages();
        }

        public void NotifyDependencyFinished()
        {
            if( Interlocked.Decrement(ref _remainingSchedulingDependencies) == 0 )
                NotifySoftDependentStages();
        }

        public StageStatus ToStageStatus()
        {
            StageStatus result = new StageStatus() { StageId = StageId };
            result.Tasks.AddRange(from task in Tasks select task.ToTaskStatus());
            return result;
        }

        private void NotifyHardDependentStages()
        {
            if( _hardDependentStages != null )
            {
                foreach( StageInfo stage in _hardDependentStages )
                    stage.NotifyDependencyFinished();
            }
        }

        private void NotifySoftDependentStages()
        {
            if( _softDependentStages != null )
            {
                foreach( StageInfo stage in _softDependentStages )
                    stage.NotifyDependencyFinished();
            }
        }
    }
}
