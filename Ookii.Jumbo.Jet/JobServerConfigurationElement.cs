// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information about the job server.
    /// </summary>
    /// <remarks>
    /// In a client application, you only need to specify the hostName and port attributes, the rest is ignored (those
    /// are only used by the JobServer itself).
    /// </remarks>
    public class JobServerConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the host name of the JobServer.
        /// </summary>
        [ConfigurationProperty("hostName", DefaultValue = "localhost", IsRequired = true, IsKey = false)]
        public string HostName
        {
            get { return (string)this["hostName"]; }
            set { this["hostName"] = value; }
        }

        /// <summary>
        /// Gets or sets the port number on which the JobServer's RPC server listens.
        /// </summary>
        [ConfigurationProperty("port", DefaultValue = 9500, IsRequired = true, IsKey = false)]
        public int Port
        {
            get { return (int)this["port"]; }
            set { this["port"] = value; }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the server should listen on both IPv6 and IPv4.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the server should listen on both IPv6 and IPv4; <see langword="false"/>
        /// if the server should listen only on IPv6 if it's available, and otherwise on IPv4.
        /// </value>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Pv"),
         ConfigurationProperty("listenIPv4AndIPv6", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool ListenIPv4AndIPv6
        {
            get { return (bool)this["listenIPv4AndIPv6"]; }
            set { this["listenIPv4AndIPv6"] = value; }
        }

        /// <summary>
        /// Gets or sets the DFS directory in which job configuration should be stored.
        /// </summary>
        [ConfigurationProperty("jetDfsPath", DefaultValue = "/JumboJet", IsRequired = false, IsKey = false)]
        public string JetDfsPath
        {
            get { return (string)this["jetDfsPath"]; }
            set { this["jetDfsPath"] = value; }
        }

        /// <summary>
        /// Gets or sets the local directory where archived jobs are stored.
        /// </summary>
        /// <value>The directory where archived jobs are stored, or <see langword="null"/> to disable job archiving.</value>
        [ConfigurationProperty("archiveDirectory", DefaultValue = null, IsRequired = false, IsKey = false)]
        public string ArchiveDirectory
        {
            get { return (string)this["archiveDirectory"]; }
            set { this["archiveDirectory"] = value; }
        }

        /// <summary>
        /// Gets or sets the scheduler to use for scheduling task.
        /// </summary>
        [ConfigurationProperty("scheduler", DefaultValue = "StagedScheduler", IsRequired = false, IsKey = false)]
        public string Scheduler
        {
            get { return (string)this["scheduler"]; }
            set { this["scheduler"] = value; }
        }

        /// <summary>
        /// Gets or sets the maximum number of times a task should be attempted if it encounters errors.
        /// </summary>
        /// <remarks>
        /// This number applies to an individual task. If the same task fails more often than this number, the job fails.
        /// </remarks>
        [ConfigurationProperty("maxTaskAttempts", DefaultValue = 5, IsRequired = false, IsKey = false)]
        public int MaxTaskAttempts
        {
            get { return (int)this["maxTaskAttempts"]; }
            set { this["maxTaskAttempts"] = value; }
        }

        /// <summary>
        /// Gets or sets the maximum number of task failures before a job fails.
        /// </summary>
        /// <value>The maximum number of task failures.</value>
        /// <remarks>
        /// This number applies to all tasks in a job. If a job encounters more task failures than this number, the job fails.
        /// </remarks>
        [ConfigurationProperty("maxTaskFailures", DefaultValue = 20, IsRequired = false, IsKey = false)]
        public int MaxTaskFailures
        {
            get { return (int)this["maxTaskFailures"]; }
            set { this["maxTaskFailures"] = value; }
        }

        /// <summary>
        /// Gets or sets the timeout, in milliseconds, after which a task server is declared dead if it has not sent a heartbeat.
        /// </summary>
        /// <value>The task server timeout.</value>
        [ConfigurationProperty("taskServerTimeout", DefaultValue = 600000, IsRequired = false, IsKey = false)]
        public int TaskServerTimeout
        {
            get { return (int)this["taskServerTimeout"]; }
            set { this["taskServerTimeout"] = value; }
        }

        /// <summary>
        /// Gets or sets the timeout, in milliseconds, after which new tasks are not scheduled on a task server if it has not sent a heartbeat.
        /// </summary>
        /// <value>The task server soft timeout.</value>
        [ConfigurationProperty("taskServerSoftTimeout", DefaultValue = 60000, IsRequired = false, IsKey = false)]
        public int TaskServerSoftTimeout
        {
            get { return (int)this["taskServerSoftTimeout"]; }
            set { this["taskServerSoftTimeout"] = value; }
        }

        /// <summary>
        /// Gets or sets the default scheduling mode for tasks with DFS input.
        /// </summary>
        /// <value>The DFS input scheduling mode.</value>
        /// <remarks>
        /// <para>
        ///   If the value of this property is set to <see cref="SchedulingMode.Default"/>, it will be treated as <see cref="SchedulingMode.MoreServers"/>.
        /// </para>
        /// </remarks>
        [ConfigurationProperty("dfsInputSchedulingMode", DefaultValue = SchedulingMode.MoreServers, IsRequired = false, IsKey = false)]
        public SchedulingMode DfsInputSchedulingMode
        {
            get { return (SchedulingMode)this["dfsInputSchedulingMode"]; }
            set { this["dfsInputSchedulingMode"] = value; }
        }

        /// <summary>
        /// Gets or sets the default scheduling mode for tasks without DFS input.
        /// </summary>
        /// <value>The DFS input scheduling mode.</value>
        /// <remarks>
        /// <para>
        ///   If the value of this property is set to <see cref="SchedulingMode.Default"/> or <see cref="SchedulingMode.OptimalLocality"/>, it will be treated as <see cref="SchedulingMode.MoreServers"/>.
        /// </para>
        /// </remarks>
        [ConfigurationProperty("nonInputSchedulingMode", DefaultValue = SchedulingMode.MoreServers, IsRequired = false, IsKey = false)]
        public SchedulingMode NonInputSchedulingMode
        {
            get { return (SchedulingMode)this["nonInputSchedulingMode"]; }
            set { this["nonInputSchedulingMode"] = value; }
        }

        /// <summary>
        /// Gets or sets the percentage of tasks of the input channel's sending stage that need to be finished before a stage can be scheduled.
        /// </summary>
        /// <value>A value between 0 and 1 that indicates the scheduling threshold. The default value is 0.4.</value>
        [ConfigurationProperty("schedulingThreshold", DefaultValue = 0.4f, IsRequired = false, IsKey = false)]
        public float SchedulingThreshold
        {
            get { return (float)this["schedulingThreshold"]; }
            set { this["schedulingThreshold"] = value; }
        }

        /// <summary>
        /// Gets or sets the IP broadcast address to use when broadcasting task completion messages.
        /// </summary>
        /// <value>The broadcast address. The default value is the IPv4 global broadcast address 255.255.255.255.</value>
        /// <remarks>
        /// This should be set to the broadcast address of your local network.
        /// </remarks>
        [ConfigurationProperty("broadcastAddress", DefaultValue = "255.255.255.255", IsRequired = false, IsKey = false)]
        public string BroadcastAddress
        {
            get { return (string)this["broadcastAddress"]; }
            set { this["broadcastAddress"] = value; }
        }

        /// <summary>
        /// Gets or sets the UDP port number to use when broadcasting task completion messages.
        /// </summary>
        /// <value>The broadcast port.</value>
        /// <remarks>
        /// Set to zero to disable broadcasting.
        /// </remarks>
        [ConfigurationProperty("broadcastPort", DefaultValue = 9550, IsRequired = false, IsKey = false)]
        public int BroadcastPort
        {
            get { return (int)this["broadcastPort"]; }
            set { this["broadcastPort"] = value; }
        }
    }
}
