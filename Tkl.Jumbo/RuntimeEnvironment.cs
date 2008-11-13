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
                string description = "CLR v" + Environment.Version.ToString();
                if( RuntimeType == RuntimeEnvironmentType.Mono )
                {
                    MethodInfo method = _monoRuntime.GetMethod("GetDisplayName", BindingFlags.InvokeMethod | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly | BindingFlags.ExactBinding);
                    if( method != null )
                        description += " (" + (string)method.Invoke(null, null) + ")";
                }
                return description;
            }
        }

        /// <summary>
        /// Launches a process using the runtime environment.
        /// </summary>
        /// <param name="startInfo">The <see cref="ProcessStartInfo"/> for the process to be launched.</param>
        /// <returns>A <see cref="Process"/> representing the launched process.</returns>
        /// <remarks>
        /// When running under Mono, this function will modify the specified <see cref="ProcessStartInfo"/> to use
        /// Mono to launch the application.
        /// </remarks>
        public static Process StartProcess(ProcessStartInfo startInfo)
        {
            if( startInfo == null )
                throw new ArgumentNullException("startInfo");
            if( RuntimeType == RuntimeEnvironmentType.Mono )
            {
                startInfo.Arguments = startInfo.FileName + " " + startInfo.Arguments;
                startInfo.FileName = "mono";
            }
            return Process.Start(startInfo);
        }
    }

}
