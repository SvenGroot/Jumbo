// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using System.Reflection;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Base class for job runners that provides interactive prompting and output file checking support.
    /// </summary>
    public abstract class BaseJobRunner : Configurable, IJobRunner
    {
        /// <summary>
        /// Gets or sets a value that indicates whether the output directory should be deleted, if it exists, before the job is executed.
        /// </summary>
        [NamedCommandLineArgument("d"), Description("Delete the output directory before running the job, if it exists.")]
        public bool DeleteOutputBeforeRun { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the job runner should wait for user input before starting the job and before exitting.
        /// </summary>
        [NamedCommandLineArgument("i"), Description("Wait for user confirmation before starting the job and before exitting.")]
        public bool IsInteractive { get; set; }

        /// <summary>
        /// Gets or sets the replication factor of the job's output files.
        /// </summary>
        /// <remarks>
        /// Derived classes should use this value with the <see cref="TaskDfsOutput"/> items of the job configuration.
        /// </remarks>
        [NamedCommandLineArgument("replication"), Description("Replication factor of the job's output files.")]
        public int ReplicationFactor { get; set; }

        /// <summary>
        /// Gets or sets the block size of the job's output files.
        /// </summary>
        /// <remarks>
        /// Derived classes should use this value with the <see cref="TaskDfsOutput"/> items of the job configuration.
        /// </remarks>
        [NamedCommandLineArgument("blockSize"), Description("Block size of the job's output files.")]
        public ByteSize BlockSize { get; set; }

        /// <summary>
        /// Gets or sets the property values that will override predefined values in the job configuration.
        /// </summary>
        /// <value>The job properties.</value>
        /// <remarks>
        /// <para>
        ///   This property is used to override the value of various properties in the job configuration
        ///   after the job runner has created it.
        /// </para>
        /// <para>
        ///   Each item in the array takes the form "PropertyName=value" or "CompoundStageId:PropertyName=value".
        ///   The first form is used to modify properties of the <see cref="JobConfiguration"/> object,
        ///   and the second form is used to modify properties of the <see cref="StageConfiguration"/> object
        ///   for the stage with the specified compound stage ID.
        /// </para>
        /// <para>
        ///   You can access properties that are more than one level deep, for instance "MyStage:OutputChannel.PartitionsPerTask=2"
        ///   is used to set the <see cref="Channels.ChannelConfiguration.PartitionsPerTask"/> property for the <see cref="StageConfiguration.OutputChannel"/>
        ///   property. This will cause an error if <see cref="StageConfiguration.OutputChannel"/> is <see langword="null"/>.
        /// </para>
        /// <para>
        ///   This method for adjusting properties can only be done with properties whose values are of a type
        ///   that can be converted to from a string. You cannot use this method to modify collection properties,
        ///   or to rename stages.
        /// </para>
        /// <para>
        ///   To apply the properties specified in this manner, call the <see cref="ApplyJobPropertiesAndSettings"/> method
        ///   in a derived class after creating your job configuration.
        /// </para>
        /// </remarks>
        [NamedCommandLineArgument("P"), Description("Modifies the value of one of the properties in the job configuration after the job has been created. Uses the format \"PropertyName=value\" or \"CompoundStageId:PropertyName=value\". You can access properties more than one level deep, e.g. \"MyStage:OutputChannel.PartitionsPerTask=2\". Can be specified more than once.")]
        public string[] JobOrStageProperties { get; set; }

        /// <summary>
        /// Gets or sets additional job or stage settings that will be defined in the job configuration.
        /// </summary>
        /// <value>The job or stage settings.</value>
        /// <remarks>
        /// <para>
        ///   You can use this property to specify or override the value of a job or stage setting
        ///   after the job runner has created the job configuration.
        /// </para>
        /// <para>
        ///   Each item in the array takes the form of "SettingName=value" for job settings, or
        ///   "CompoundStageId:SettingName=value" for stage settings.
        /// </para>
        /// <para>
        ///   If the setting is already defined, its value will be modified to the value specified
        ///   in this property.
        /// </para>
        /// <para>
        ///   To apply the settings specified in this manner, call the <see cref="ApplyJobPropertiesAndSettings"/> method
        ///   in a derived class after creating your job configuration.
        /// </para>
        /// </remarks>
        [NamedCommandLineArgument("D"), Description("Defines or overrides a job or stage setting in the job configuration. Uses the format \"SettingName=value\" or \"CompoundStageId:SettingName=value\". Can be specified more than once.")]
        public string[] JobOrStageSettings { get; set; }

        #region IJobRunner Members

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public abstract Guid RunJob();

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        public virtual void FinishJob(bool success)
        {
            PromptIfInteractive(false);
        }

        #endregion

        /// <summary>
        /// Prompts the user to start or exit, if <see cref="IsInteractive"/> is <see langword="true"/>.
        /// </summary>
        /// <param name="promptForStart"><see langword="true"/> to prompt for the start of the job; <see langword="false"/>
        /// to prompt for exit.</param>
        protected void PromptIfInteractive(bool promptForStart)
        {
            if( IsInteractive )
            {
                if( promptForStart )
                    Console.WriteLine("Press any key to start . . .");
                else
                    Console.WriteLine("Press any key to exit . . .");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// If <see cref="DeleteOutputBeforeRun"/> is <see langword="true"/>, deletes the output path and then re-creates it; otherwise,
        /// checks if the output path exists and creates it if it doesn't exist and fails if it does. Uses the value of the <see cref="Configurable.DfsConfiguration"/>
        /// property to access the DFS.
        /// </summary>
        /// <param name="outputPath">The directory where the job's output will be stored.</param>
        protected void CheckAndCreateOutputPath(string outputPath)
        {
            CheckAndCreateOutputPath(new DfsClient(DfsConfiguration), outputPath);
        }

        /// <summary>
        /// If <see cref="DeleteOutputBeforeRun"/> is <see langword="true"/>, deletes the output path and then re-creates it; otherwise,
        /// checks if the output path exists and creates it if it doesn't exist and fails if it does.
        /// </summary>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the Distributed File System.</param>
        /// <param name="outputPath">The directory where the job's output will be stored.</param>
        protected void CheckAndCreateOutputPath(DfsClient dfsClient, string outputPath)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");

            if( DeleteOutputBeforeRun )
            {
                dfsClient.NameServer.Delete(outputPath, true);
            }
            else
            {
                DfsDirectory outputDir = dfsClient.NameServer.GetDirectoryInfo(outputPath);
                if( outputDir != null )
                    throw new ArgumentException("The specified output path already exists on the DFS.", "outputPath");
            }
            dfsClient.NameServer.CreateDirectory(outputPath);
        }

        /// <summary>
        /// Sets the <see cref="ReplicationFactor"/> and <see cref="BlockSize"/> for the specified stage's DFS output.
        /// </summary>
        /// <param name="stage">The stage whose DFS output to configure.</param>
        protected void ConfigureDfsOutput(StageConfiguration stage)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( stage.DfsOutput == null )
                throw new ArgumentException("Stage has no DFS output", "stage");

            if( ReplicationFactor < 0 )
                throw new InvalidOperationException("Replication factor may not be less than zero.");
            if( BlockSize.Value < 0 )
                throw new InvalidOperationException("Block size may not be less than zero.");
            if( BlockSize.Value > Int32.MaxValue )
                throw new InvalidOperationException("Block size may not be larger than 2GB.");

            stage.DfsOutput.BlockSize = (int)BlockSize.Value;
            stage.DfsOutput.ReplicationFactor = ReplicationFactor;
        }

        /// <summary>
        /// Gets a <see cref="FileSystemEntry"/> instance for the specified path, or throws an exception if the input doesn't exist.
        /// </summary>
        /// <param name="dfsClient">The <see cref="DfsClient"/> used to access the Distributed File System.</param>
        /// <param name="inputPath">The input file or directory.</param>
        /// <returns>A <see cref="FileSystemEntry"/> instance for the specified path</returns>
        protected static FileSystemEntry GetInputFileSystemEntry(DfsClient dfsClient, string inputPath)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            FileSystemEntry input = dfsClient.NameServer.GetFileSystemEntryInfo(inputPath);
            if( input == null )
                throw new ArgumentException("The specified input path doesn't exist.", "inputPath");
            return input;
        }

        /// <summary>
        /// Adds the values of properties marked with the <see cref="JobSettingAttribute"/> to the <see cref="JobConfiguration.JobSettings"/> dictionary, 
        /// applies properties set by the <see cref="JobOrStageProperties"/> property, and adds settings defined by the <see cref="JobOrStageSettings"/> property,
        /// and .
        /// </summary>
        /// <param name="jobConfiguration">The job configuration.</param>
        protected void ApplyJobPropertiesAndSettings(JobConfiguration jobConfiguration)
        {
            if( jobConfiguration == null )
                throw new ArgumentNullException("jobConfiguration");

            ApplySettingProperties(jobConfiguration);

            ApplyJobProperties(jobConfiguration);

            ApplyJobOrStageSettings(jobConfiguration);
        }

        private void ApplySettingProperties(JobConfiguration jobConfiguration)
        {
            PropertyInfo[] props = GetType().GetProperties();
            foreach( PropertyInfo prop in props )
            {
                JobSettingAttribute attribute = (JobSettingAttribute)Attribute.GetCustomAttribute(prop, typeof(JobSettingAttribute));
                if( attribute != null )
                {
                    string key = attribute.Key;
                    if( key == null )
                        key = string.Format(CultureInfo.InvariantCulture, "{0}.{1}", prop.DeclaringType.Name, prop.Name);

                    jobConfiguration.AddSetting(key, prop.GetValue(this, null));
                }
            }
        }

        private void ApplyJobOrStageSettings(JobConfiguration jobConfiguration)
        {
            if( JobOrStageSettings != null )
            {
                foreach( string setting in JobOrStageSettings )
                {
                    string compoundStageId;
                    string settingName;
                    string settingValue;
                    ParsePropertyOrSetting(setting, out compoundStageId, out settingName, out settingValue);

                    SettingsDictionary target = null;
                    if( compoundStageId == null )
                    {
                        if( jobConfiguration.JobSettings == null )
                            jobConfiguration.JobSettings = new SettingsDictionary();
                        target = jobConfiguration.JobSettings;
                    }
                    else
                    {
                        StageConfiguration stage = jobConfiguration.GetStageWithCompoundId(compoundStageId);
                        if( stage == null )
                            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} specified in command line argument -D:{1} does not exist.", compoundStageId, setting));
                        if( stage.StageSettings == null )
                            stage.StageSettings = new SettingsDictionary();
                        target = stage.StageSettings;
                    }

                    target[settingName] = settingValue;
                }
            }
        }

        private void ApplyJobProperties(JobConfiguration jobConfiguration)
        {
            if( JobOrStageProperties != null )
            {
                foreach( string prop in JobOrStageProperties )
                {
                    ApplyJobProperty(jobConfiguration, prop);
                }
            }
        }

        private static void ApplyJobProperty(JobConfiguration job, string prop)
        {
            string compoundStageId;
            string propName;
            string propValue;
            ParsePropertyOrSetting(prop, out compoundStageId, out propName, out propValue);

            object target = job;
            if( compoundStageId != null )
            {
                target = job.GetStageWithCompoundId(compoundStageId);
                if( target == null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} specified in command line argument -P:{1} does not exist.", compoundStageId, prop));
            }

            ApplyJobProperty(target, propName, propValue);
        }

        private static void ParsePropertyOrSetting(string propOrSetting, out string compoundStageId, out string name, out string value)
        {
            compoundStageId = null;

            int colonIndex = propOrSetting.IndexOf(':');
            if( colonIndex >= 0 )
            {
                compoundStageId = propOrSetting.Substring(0, colonIndex);
            }

            int equalsIndex = propOrSetting.IndexOf('=', colonIndex + 1);
            if( equalsIndex < 0 )
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Job property specified in command line argument -P:{0} has no value.", propOrSetting));

            name = propOrSetting.Substring(colonIndex + 1, equalsIndex - colonIndex - 1);
            value = propOrSetting.Substring(equalsIndex + 1);
        }

        private static void ApplyJobProperty(object target, string path, string value)
        {
            string[] pathItems = path.Split('.');
            PropertyInfo prop = null;
            foreach( string propName in pathItems )
            {
                if( prop != null )
                {
                    if( !prop.CanRead )
                        throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Property {0} in property path {1} is not readable.", prop.Name, path));
                    target = prop.GetValue(target, null);
                }
                prop = target.GetType().GetProperty(propName);
                if( prop == null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Property {0} in property path {1} is does not exist.", propName, path));
            }

            if( !prop.CanWrite )
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Property {0} is not writable.", path));

            TypeConverter converter = TypeDescriptor.GetConverter(prop.PropertyType);
            object convertedValue = converter.ConvertFromString(value);

            prop.SetValue(target, convertedValue, null);
        }
    }
}
