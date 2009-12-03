﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Management;
using System.ComponentModel;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides information about the runtime environment of the application.
    /// </summary>
    public static class RuntimeEnvironment
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RuntimeEnvironment));
        private static readonly Type _monoRuntime = typeof(object).Assembly.GetType("Mono.Runtime");
        private static string _operatingSystemDescription;

        /// <summary>
        /// Gets a value that indicates what runtime the application is running on.
        /// </summary>
        public static RuntimeEnvironmentType RuntimeType
        {
            get
            {
                return _monoRuntime == null ? RuntimeEnvironmentType.DotNet : RuntimeEnvironmentType.Mono;
            }
        }

        /// <summary>
        /// Gets a description of the runtime environment, including the version number.
        /// </summary>
        public static string Description
        {
            get
            {
                string additionalInfo = string.Empty;
                if( RuntimeType == RuntimeEnvironmentType.Mono )
                {
                    MethodInfo method = _monoRuntime.GetMethod("GetDisplayName", BindingFlags.InvokeMethod | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly | BindingFlags.ExactBinding);
                    if( method != null )
                        additionalInfo = (string)method.Invoke(null, null);
                }
                else
                    additionalInfo = "Microsoft .Net";
                return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0} ({1})", Environment.Version, additionalInfo);
            }
        }

        /// <summary>
        /// Gets a description of the operating system, including the version number.
        /// </summary>
        public static string OperatingSystemDescription
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                if( _operatingSystemDescription == null )
                {
                    string description = null;
                    switch( Environment.OSVersion.Platform )
                    {
                    case PlatformID.Win32NT:
                        description = GetOSDescriptionWindows();
                        break;
                    case PlatformID.Unix:
                        description = GetOSDescriptionUnix();
                        break;
                    }

                    if( description == null )
                        _operatingSystemDescription = Environment.OSVersion.ToString();
                    else
                        _operatingSystemDescription = string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0} ({1})", description, Environment.OSVersion);
                }
                return _operatingSystemDescription;
            }
        }

        /// <summary>
        /// Modifies a <see cref="ProcessStartInfo"/> to use the runtime environment.
        /// </summary>
        /// <param name="startInfo">The <see cref="ProcessStartInfo"/>.</param>
        /// <param name="profileOutputFile">The file to write the profiler output to. Specify <see langword="null"/> to disable profiling.</param>
        /// <param name="profileOptions">Additional options to pass to the profiler.</param>
        /// <remarks>
        /// <para>
        ///   When running under Mono, this function will modify the specified <see cref="ProcessStartInfo"/> to use
        ///   Mono to launch the application.
        /// </para>
        /// <para>
        ///   Profiling is enabled when <paramref name="profileOutputFile"/> is not <see langword="null"/>. Profiling is supported
        ///   only on Mono.
        /// </para>
        /// </remarks>
        public static void ModifyProcessStartInfo(ProcessStartInfo startInfo, string profileOutputFile, string profileOptions)
        {
            if( startInfo == null )
                throw new ArgumentNullException("startInfo");
            if( RuntimeType == RuntimeEnvironmentType.Mono )
            {
                startInfo.Arguments = startInfo.FileName + " " + startInfo.Arguments;
                if( !string.IsNullOrEmpty(profileOutputFile) )
                {
                    if( !string.IsNullOrEmpty(profileOptions) )
                        profileOptions += ",";

                    startInfo.Arguments = string.Format(System.Globalization.CultureInfo.InvariantCulture, "--profile=default:{0}file={1} {2}", profileOptions, profileOutputFile, startInfo.Arguments);
                }
                startInfo.FileName = "mono";
            }
        }

        /// <summary>
        /// Writes environemnt information to the specified log.
        /// </summary>
        /// <param name="log">The log to write the information to.</param>
        public static void LogEnvironmentInformation(this log4net.ILog log)
        {
            if( log == null )
                throw new ArgumentNullException("log");

            if( log.IsInfoEnabled )
            {
                log.InfoFormat("Jumbo Version: {0}", Assembly.GetExecutingAssembly().GetName().Version);
                Assembly entry = Assembly.GetEntryAssembly();
                if( entry != null ) // entry is null when running under nunit.
                    log.InfoFormat("{0} Version: {1}", entry.GetName().Name, entry.GetName().Version);
                log.InfoFormat("   OS Version: {0}", OperatingSystemDescription);
                log.InfoFormat("  CLR Version: {0} ({1} bit runtime)", Description, IntPtr.Size * 8);
            }
        }

        /// <summary>
        /// Sets a file's permissions to indicate it's executable.
        /// </summary>
        /// <param name="fileName">The file whose permissions to change.</param>
        /// <remarks>
        /// <para>
        ///   On Unix platforms, this method will change the file permissions of the specified file
        ///   to include the user execute bit.
        /// </para>
        /// <para>
        ///   On Windows, this method does nothing.
        /// </para>
        /// </remarks>
        public static void MarkFileAsExecutable(string fileName)
        {
            if( fileName == null )
                throw new ArgumentNullException("fileName");

            if( RuntimeType == RuntimeEnvironmentType.Mono && Environment.OSVersion.Platform == PlatformID.Unix )
            {
                const string unixFileInfoTypeName = "Mono.Unix.UnixFileInfo, Mono.Posix, Version=2.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756";

                Type unixFileInfoType = Type.GetType(unixFileInfoTypeName);
                PropertyInfo fileAccessPermissionsProperty = unixFileInfoType.GetProperty("FileAccessPermissions");
                object unixFile = Activator.CreateInstance(unixFileInfoType, fileName);

                object mode = fileAccessPermissionsProperty.GetValue(unixFile, null);
                Type fileAccessPermissionsType = mode.GetType();
                int oldMode = Convert.ToInt32(mode, System.Globalization.CultureInfo.InvariantCulture);
                int executeBit = Convert.ToInt32(fileAccessPermissionsType.GetField("UserExecute").GetValue(null), System.Globalization.CultureInfo.InvariantCulture);
                int newMode = oldMode | executeBit;

                if( newMode != oldMode )
                {
                    object newModeValue = Enum.ToObject(fileAccessPermissionsType, newMode);
                    _log.DebugFormat("Changing file permissions for file '{0}' from {1} to {2}.", fileName, mode, newModeValue);

                    fileAccessPermissionsProperty.SetValue(unixFile, newModeValue, null);
                }
            }
        }

        private static string GetOSDescriptionWindows()
        {
            // Use WMI to get the OS name.
            SelectQuery query = new SelectQuery("Win32_OperatingSystem");
            ManagementObjectSearcher searcher = new ManagementObjectSearcher(query);

            foreach( ManagementBaseObject obj in searcher.Get() )
            {
                return (string)obj["Caption"];
            }

            return null;
        }

        private static string GetOSDescriptionUnix()
        {
            try
            {
                // This will only work on Linux, but that's ok.
                ProcessStartInfo psi = new ProcessStartInfo("lsb_release", "-d -s")
                {
                    UseShellExecute = false,
                    RedirectStandardOutput = true
                };

                using( Process p = Process.Start(psi) )
                {
                    return p.StandardOutput.ReadToEnd().Trim();
                }
            }
            catch( Win32Exception )
            {
                return null;
            }
        }
    }

}
