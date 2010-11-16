// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;

namespace JobServerApplication.Scheduling
{
    static class DataServerMap
    {
        private static readonly Dictionary<string, ServerAddress[]> _serverMap = new Dictionary<string, ServerAddress[]>();
        private static DateTime _mapUpdatedTime = DateTime.MinValue;
        private const int _maxMapAgeSeconds = 180;

        public static ServerAddress[] GetDataServersForTaskServer(ServerAddress taskServer, IEnumerable<TaskServerInfo> taskServers, DfsClient dfsClient)
        {
            lock( _serverMap )
            {
                if( (DateTime.Now - _mapUpdatedTime).TotalSeconds > _maxMapAgeSeconds )
                {
                    BuildServerMap(taskServers, dfsClient);
                }

                ServerAddress[] dataServers;
                if( _serverMap.TryGetValue(taskServer.HostName, out dataServers) )
                    return dataServers;
                else
                {
                    BuildServerMap(taskServers, dfsClient);
                    return _serverMap[taskServer.HostName];
                }
            }
        }

        private static void BuildServerMap(IEnumerable<TaskServerInfo> taskServers, DfsClient dfsClient)
        {
            DfsMetrics metrics = dfsClient.NameServer.GetMetrics();

            _serverMap.Clear();

            foreach( TaskServerInfo taskServer in taskServers )
            {
                if( !_serverMap.ContainsKey(taskServer.Address.HostName) )
                {
                    var dataServers = from server in metrics.DataServers
                                      where server.Address.HostName == taskServer.Address.HostName
                                      select server.Address;
                    _serverMap.Add(taskServer.Address.HostName, dataServers.ToArray());
                }

                _mapUpdatedTime = DateTime.Now;
            }
        }
    }
}
