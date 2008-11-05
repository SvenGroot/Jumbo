using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Diagnostics;

namespace Tkl.Jumbo.Dfs.Test
{
    [SetUpFixture]
    public class TestSetup
    {
        [SetUp]
        public void Setup()
        {
            if( Environment.GetEnvironmentVariable("JUMBO_TRACE") == "true" )
            {
                Trace.Listeners.Clear();
                Trace.Listeners.Add(new ConsoleTraceListener());
                Utilities.TraceLineAndFlush("Listeners configured");
            }
        }
    }
}
