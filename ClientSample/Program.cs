using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting;

namespace ClientSample
{
    class Program
    {
        static void Main(string[] args)
        {
            RemotingConfiguration.Configure("ClientSample.exe.config", false);
            var types = RemotingConfiguration.GetRegisteredWellKnownClientTypes();
            IClientProtocol nameServer = (IClientProtocol)Activator.GetObject(types[0].ObjectType, types[0].ObjectUrl);
            //nameServer.CreateDirectory("/test/foo");
            //nameServer.CreateFile("/test/bar");
            File f = nameServer.GetFileInfo("/test/bar");
            Console.WriteLine(f.Name);
            Console.WriteLine(f.DateCreated);
            Console.ReadKey();
        }
    }
}
