using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Enables the use of <see cref="Type.GetType(string)"/> to resolve types in assemblies loaded with <see cref="Assembly.LoadFrom(string)"/>.
    /// </summary>
    public class AssemblyResolver
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AssemblyResolver"/> class.
        /// </summary>
        public AssemblyResolver()
        {
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
        }

        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            // The TaskHost wants to use Type.GetType to instantiate various types, and it wants to include the
            // assemblies loaded by Assembly.LoadFrom, which isn't done by default. We'll do that here.
            Assembly result = (from assembly in ((AppDomain)sender).GetAssemblies()
                               where assembly.FullName == args.Name
                               select assembly).SingleOrDefault();
            return result;
        }
    }
}
