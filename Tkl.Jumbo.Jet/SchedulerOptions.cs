// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides job configuration options to configure the behaviour of the scheduler.
    /// </summary>
    [XmlType("SchedulerOptions", Namespace=JobConfiguration.XmlNamespace)]
    public sealed class SchedulerOptions
    {
        private int _maximumDataDistance = 2;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchedulerOptions"/> class.
        /// </summary>
        public SchedulerOptions()
        {
            SpreadDfsInputTasks = true;
            SpreadNonInputTasks = true;
        }

        /// <summary>
        /// Gets or sets the maximum distance from the input data for a DFS input task.
        /// </summary>
        /// <value>Zero to allow only data-local tasks, one to also allow rack-local tasks, two or higher to also allow non-local tasks. The default value is two.</value>
        [XmlAttribute("maximumDataDistance")]
        public int MaximumDataDistance
        {
            get { return _maximumDataDistance; }
            set 
            {
                if( _maximumDataDistance < 0 )
                    throw new ArgumentOutOfRangeException("value");
                _maximumDataDistance = value; 
            }
        }
        

        /// <summary>
        /// Gets or sets a value indicating whether to spread the DFS input tasks over as many servers as possible.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the DFS input tasks are spread over as many servers as possible; otherwise, <see langword="false"/>. The default
        /// 	value is <see langword="true"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   When this property is <see langword="false"/>, the scheduler will assign a single task to a server and then move on to the next server, repeatedly iterating over
        ///   the servers until there are no more tasks it can schedule.
        /// </para>
        /// <para>
        ///   When this property is <see langword="false"/>, the scheduler will assign as many tasks as possible to a server before moving on to the next.
        /// </para>
        /// </remarks>
        [XmlAttribute("spreadDfsInputTasks")]
        public bool SpreadDfsInputTasks { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to spread tasks that do not read from the DFS over as many servers as possible.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if tasks that do not read from the DFS are spread over as many servers as possible; otherwise, <see langword="false"/>. The default
        /// 	value is <see langword="true"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   When this property is <see langword="false"/>, the scheduler will assign a single task to a server and then move on to the next server, repeatedly iterating over
        ///   the servers until there are no more tasks it can schedule.
        /// </para>
        /// <para>
        ///   When this property is <see langword="false"/>, the scheduler will assign as many tasks as possible to a server before moving on to the next.
        /// </para>
        /// </remarks>
        [XmlAttribute("spreadNonInputTasks")]
        public bool SpreadNonInputTasks { get; set; }
    }
}
