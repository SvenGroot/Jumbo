using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.Reflection;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides information about a job runner.
    /// </summary>
    public sealed class JobRunnerInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobRunnerInfo));

        private readonly Type _jobRunnerType;
        private readonly JobRunnerArgument[] _arguments;
        private readonly int _minimumArgumentCount;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobRunnerInfo"/> class.
        /// </summary>
        /// <param name="type">The type of the job runner.</param>
        public JobRunnerInfo(Type type)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( !type.GetInterfaces().Contains(typeof(IJobRunner)) )
                throw new ArgumentException("Specified type is not a job runner.", "type");

            _jobRunnerType = type;

            ConstructorInfo[] ctors = type.GetConstructors();
            if( ctors.Length < 1 )
                throw new ArgumentException("Job runner type does not have any public constructors.", "type");
            // If there's more than one, we just use the first one; job runners should normally have only one constructor.
            ConstructorInfo ctor = ctors[0];
            ParameterInfo[] parameters = ctor.GetParameters();
            _arguments = new JobRunnerArgument[parameters.Length];
            bool hasOptionalAttribute = false;
            for( int x = 0; x < parameters.Length; ++x )
            {
                ParameterInfo parameter = parameters[x];
                OptionalArgumentAttribute optionalAttribute = (OptionalArgumentAttribute)Attribute.GetCustomAttribute(parameter, typeof(OptionalArgumentAttribute));
                if( optionalAttribute == null && hasOptionalAttribute )
                    throw new ArgumentException("Job runner constructor cannot have non-optional arguments after an optional argument.");
                else if( optionalAttribute != null && !hasOptionalAttribute )
                {
                    hasOptionalAttribute = true;
                    _minimumArgumentCount = x;
                }
                _arguments[x] = new JobRunnerArgument(parameter.Name, parameter.ParameterType, optionalAttribute != null, optionalAttribute == null ? null : optionalAttribute.DefaultValue);
            }
        }

        /// <summary>
        /// Gets the name of the job runner.
        /// </summary>
        public string Name
        {
            get { return _jobRunnerType.Name; }
        }

        /// <summary>
        /// Gets a description of the job runner.
        /// </summary>
        public string Description
        {
            get
            {
                DescriptionAttribute description = (DescriptionAttribute)Attribute.GetCustomAttribute(_jobRunnerType, typeof(DescriptionAttribute));
                return description == null ? "" : description.Description;
            }
        }

        /// <summary>
        /// Gets a string describing the command line usage of the job runner.
        /// </summary>
        public string Usage
        {
            get
            {
                StringBuilder usage = new StringBuilder(Name);
                foreach( JobRunnerArgument argument in _arguments )
                {
                    usage.Append(" ");
                    if( argument.IsOptional )
                        usage.Append("[");
                    else
                        usage.Append("<");
                    usage.Append(argument.Name);
                    if( argument.IsOptional )
                    {
                        usage.Append("=");
                        usage.Append(argument.DefaultValue);
                        usage.Append("]");
                    }
                    else
                        usage.Append(">");
                }
                return usage.ToString();
            }
        }

        /// <summary>
        /// Gets all the job runners defined in the specified assembly.
        /// </summary>
        /// <param name="assembly">The assembly to check for job runners.</param>
        /// <returns>An array holding the job runners in the assembly.</returns>
        public static JobRunnerInfo[] GetJobRunners(Assembly assembly)
        {
            if( assembly == null )
                throw new ArgumentNullException("assembly");

            Type[] types = assembly.GetTypes();
            return (from type in types
                    where type.IsPublic && type.IsClass && type.GetInterfaces().Contains(typeof(IJobRunner))
                    select new JobRunnerInfo(type)).ToArray();
        }

        /// <summary>
        /// Gets the specified job runner from the specified assembly.
        /// </summary>
        /// <param name="assembly">The assembly to check for the job runner.</param>
        /// <param name="name">The name of the job runner.</param>
        /// <returns>The <see cref="JobRunnerInfo"/> for the specified job runner, or <see langword="null" /> if it was not found.</returns>
        public static JobRunnerInfo GetJobRunner(Assembly assembly, string name)
        {
            if( assembly == null )
                throw new ArgumentNullException("assembly");
            if( name == null )
                throw new ArgumentNullException("name");

            Type[] types = assembly.GetTypes();
            return (from type in types
                    where type.IsPublic && type.IsClass && type.GetInterfaces().Contains(typeof(IJobRunner)) && string.Equals(type.Name, name, StringComparison.OrdinalIgnoreCase)
                    select new JobRunnerInfo(type)).SingleOrDefault();
        }

        /// <summary>
        /// Creates an instance of the job runner.
        /// </summary>
        /// <param name="dfsConfiguration">The Jumbo DFS configuration for the job.</param>
        /// <param name="jetConfiguration">The Jumbo Jet configuration for the job.</param>
        /// <param name="args">The arguments for the job.</param>
        /// <returns>An instance of the job runner, or <see langword="null" /> if the incorrect number of arguments was specified.</returns>
        public IJobRunner CreateInstance(DfsConfiguration dfsConfiguration, JetConfiguration jetConfiguration, string[] args)
        {
            if( dfsConfiguration == null )
                throw new ArgumentNullException("dfsConfiguration");
            if( jetConfiguration == null )
                throw new ArgumentNullException("jetConfiguration");
            if( args == null )
                throw new ArgumentNullException("args");
            if( args.Length < _minimumArgumentCount || args.Length > _arguments.Length )
                return null;

            StringBuilder logMessage = new StringBuilder("Creating job runner for job ");
            logMessage.Append(Name);
            object[] typedArguments = new object[_arguments.Length];
            for( int x = 0; x < _arguments.Length; ++x )
            {
                if( x < args.Length )
                    typedArguments[x] = _arguments[x].ConvertToArgumentType(args[x]);
                else
                    typedArguments[x] = _arguments[x].DefaultValue;
                logMessage.Append(", ");
                logMessage.Append(_arguments[x].Name);
                logMessage.Append(" = ");
                logMessage.Append(typedArguments[x]);
            }

            _log.Info(logMessage.ToString());
            return (IJobRunner)JetActivator.CreateInstance(_jobRunnerType, dfsConfiguration, jetConfiguration, null, typedArguments);
        }

        /// <summary>
        /// Creates an instance of the job runner with the configuration from the app.config file.
        /// </summary>
        /// <param name="args">The arguments for the job.</param>
        /// <returns>An instance of the job runner, or <see langword="null" /> if the incorrect number of arguments was specified.</returns>
        public IJobRunner CreateInstance(string[] args)
        {
            return CreateInstance(DfsConfiguration.GetConfiguration(), JetConfiguration.GetConfiguration(), args);
        }
    }
}
