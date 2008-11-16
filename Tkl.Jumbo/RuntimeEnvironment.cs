using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Diagnostics;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides information about the runtime environment of the application.
    /// </summary>
    public static class RuntimeEnvironment
    {
        private static readonly Type _monoRuntime = typeof(object).Assembly.GetType("Mono.Runtime");

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
        /// Modifies a <see cref="ProcessStartInfo"/> to use the runtime environment.
        /// </summary>
        /// <param name="startInfo">The <see cref="ProcessStartInfo"/>.</param>
        /// <remarks>
        /// When running under Mono, this function will modify the specified <see cref="ProcessStartInfo"/> to use
        /// Mono to launch the application.
        /// </remarks>
        public static void ModifyProcessStartInfo(ProcessStartInfo startInfo)
        {
            if( startInfo == null )
                throw new ArgumentNullException("startInfo");
            if( RuntimeType == RuntimeEnvironmentType.Mono )
            {
                startInfo.Arguments = startInfo.FileName + " " + startInfo.Arguments;
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
                log.InfoFormat("   OS Version: {0}", Environment.OSVersion);
                log.InfoFormat("  CLR Version: {0} ({1} bit runtime)", Description, IntPtr.Size * 8);
            }
        }
    }

}
