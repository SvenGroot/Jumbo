﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides client access to the Jumbo Jet distributed execution engine.
    /// </summary>
    public class JetClient
    {
        private const string _jobServerUrlFormat = "tcp://{0}:{1}/JobServer";
        private const string _taskServerUrlFormat = "tcp://{0}:{1}/TaskServer";

        static JetClient()
        {
            RpcHelper.RegisterClientChannel();
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server via the heartbeat protocol.
        /// </summary>
        /// <returns>An object implementing <see cref="IJobServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerHeartbeatProtocol CreateJobServerHeartbeatClient()
        {
            return CreateJobServerHeartbeatClient(JetConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server via the heartbeat protocol
        /// using the specified configuration.
        /// </summary>
        /// <param name="config">A <see cref="JetConfiguration"/> that provides the job server configuration to use.</param>
        /// <returns>An object implementing <see cref="IJobServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerHeartbeatProtocol CreateJobServerHeartbeatClient(JetConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            return CreateJobServerClientInternal<IJobServerHeartbeatProtocol>(config.JobServer.HostName, config.JobServer.Port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server.
        /// </summary>
        /// <returns>An object implementing <see cref="IJobServerClientProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerClientProtocol CreateJobServerClient()
        {
            return CreateJobServerClient(JetConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server.
        /// </summary>
        /// <param name="config">A <see cref="JetConfiguration"/> that provides the job server configuration to use.</param>
        /// <returns>An object implementing <see cref="IJobServerClientProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerClientProtocol CreateJobServerClient(JetConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            return CreateJobServerClientInternal<IJobServerClientProtocol>(config.JobServer.HostName, config.JobServer.Port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server.
        /// </summary>
        /// <param name="hostName">The host name of the job server.</param>
        /// <param name="port">The port at which the job server is listening.</param>
        /// <returns>An object implementing <see cref="IJobServerClientProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerClientProtocol CreateJobServerClient(string hostName, int port)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");

            return CreateJobServerClientInternal<IJobServerClientProtocol>(hostName, port);
        }

        /// <summary>
        /// Creates a client object that can be used by a task host to communicate with its task server.
        /// </summary>
        /// <param name="port">The port at which the task server is listening.</param>
        /// <returns>An object implementing <see cref="ITaskServerUmbilicalProtocol"/> that is a proxy class for
        /// communicating with the task server via RPC.</returns>
        public static ITaskServerUmbilicalProtocol CreateTaskServerUmbilicalClient(int port)
        {
            return CreateTaskServerClientInternal<ITaskServerUmbilicalProtocol>("localhost", port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with its task server server.
        /// </summary>
        /// <param name="address">The address of the task server.</param>
        /// <returns>An object implementing <see cref="ITaskServerClientProtocol"/> that is a proxy class for
        /// communicating with the task server via RPC.</returns>
        public static ITaskServerClientProtocol CreateTaskServerClient(ServerAddress address)
        {
            if( address == null )
                throw new ArgumentNullException("address");

            return CreateTaskServerClientInternal<ITaskServerClientProtocol>(address.HostName, address.Port);
        }
        
        private static T CreateJobServerClientInternal<T>(string hostName, int port)
        {
            string url = string.Format(System.Globalization.CultureInfo.InvariantCulture, _jobServerUrlFormat, hostName, port);
            return (T)Activator.GetObject(typeof(T), url);
        }

        private static T CreateTaskServerClientInternal<T>(string hostName, int port)
        {
            string url = string.Format(System.Globalization.CultureInfo.InvariantCulture, _taskServerUrlFormat, hostName, port);
            return (T)Activator.GetObject(typeof(T), url);
        }
    }
}
