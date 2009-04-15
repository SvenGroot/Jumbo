using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides object activation services for Jumbo Jet.
    /// </summary>
    public static class JetActivator
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JetActivator));

        /// <summary>
        /// Creates an instance of the specified type and configures it.
        /// </summary>
        /// <param name="type">The type to instantiate.</param>
        /// <param name="dfsConfiguration">The configuration used to access the distributed file system.</param>
        /// <param name="jetConfiguration">The configuration used to access Jet.</param>
        /// <param name="jobConfiguration">The configuration for the current Job.</param>
        /// <param name="args">The arguments to pass to the object's constructor.</param>
        /// <returns>An instance of the specified type with the configuration applied to it.</returns>
        /// <remarks>
        /// <para>
        ///   This function instantiates the type specified in <paramref name="type"/>, then checks if the object
        ///   implements <see cref="IConfigurable"/> and if so, applies the configuration to it.
        /// </para>
        /// </remarks>
        public static object CreateInstance(Type type, DfsConfiguration dfsConfiguration, JetConfiguration jetConfiguration, JobConfiguration jobConfiguration, params object[] args)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            _log.DebugFormat("Creating instance of type {0}.", type.AssemblyQualifiedName);
            object instance = Activator.CreateInstance(type, args);

            ApplyConfiguration(instance, dfsConfiguration, jetConfiguration, jobConfiguration);

            return instance;
        }

        /// <summary>
        /// Creates an instance of the specified type and configures it.
        /// </summary>
        /// <param name="type">The type to instantiate.</param>
        /// <param name="taskExecution">A <see cref="TaskExecutionUtility"/> instance that contains the configuration to use.</param>
        /// <param name="args">The arguments to pass to the object's constructor.</param>
        /// <returns>An instance of the specified type with the configuration applied to it.</returns>
        /// <remarks>
        /// <para>
        ///   This function instantiates the type specified in <paramref name="type"/>, then checks if the object
        ///   implements <see cref="IConfigurable"/> and if so, applies the configuration to it.
        /// </para>
        /// </remarks>
        public static object CreateInstance(Type type, TaskExecutionUtility taskExecution, params object[] args)
        {
            if( taskExecution == null )
                return CreateInstance(type, null, null, null, args);
            else
                return CreateInstance(type, taskExecution.DfsClient.Configuration, taskExecution.JetClient.Configuration, taskExecution.JobConfiguration, args);
        }

        /// <summary>
        /// Applies the specified configuration to the specified cobject.
        /// </summary>
        /// <param name="obj">The object to configure.</param>
        /// <param name="dfsConfiguration">The configuration used to access the distributed file system.</param>
        /// <param name="jetConfiguration">The configuration used to access Jet.</param>
        /// <param name="jobConfiguration">The configuration for the current Job.</param>
        /// <remarks>
        /// <para>
        ///   This function checks if the object implements <see cref="IConfigurable"/> and if so, applies the configuration to it.
        /// </para>
        /// </remarks>
        public static void ApplyConfiguration(object obj, DfsConfiguration dfsConfiguration, JetConfiguration jetConfiguration, JobConfiguration jobConfiguration)
        {
            if( obj == null )
                throw new ArgumentNullException("obj");

            IConfigurable configurable = obj as IConfigurable;
            if( configurable != null )
            {
                if( _log.IsDebugEnabled )
                    _log.DebugFormat("Applying configuration to configurable object of type {0}.", obj.GetType().AssemblyQualifiedName);
                configurable.DfsConfiguration = dfsConfiguration;
                configurable.JetConfiguration = jetConfiguration;
                configurable.JobConfiguration = jobConfiguration;
            }
        }
    }
}
