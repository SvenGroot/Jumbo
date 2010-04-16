using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Messaging;

namespace Tkl.Jumbo
{
    class ServerChannelSink : BaseChannelObjectWithProperties, IServerChannelSink
    {
        private readonly IServerChannelSink _nextChannelSink;

        public ServerChannelSink(IServerChannelSink nextChannelSink)
        {
            _nextChannelSink = nextChannelSink;
        }

        #region IServerChannelSink Members

        public void AsyncProcessResponse(IServerResponseChannelSinkStack sinkStack, object state, System.Runtime.Remoting.Messaging.IMessage msg, ITransportHeaders headers, System.IO.Stream stream)
        {
        }

        public System.IO.Stream GetResponseStream(IServerResponseChannelSinkStack sinkStack, object state, System.Runtime.Remoting.Messaging.IMessage msg, ITransportHeaders headers)
        {
            return null;
        }

        public IServerChannelSink NextChannelSink
        {
            get { return _nextChannelSink; }
        }

        public ServerProcessing ProcessMessage(IServerChannelSinkStack sinkStack, System.Runtime.Remoting.Messaging.IMessage requestMsg, ITransportHeaders requestHeaders, System.IO.Stream requestStream, out System.Runtime.Remoting.Messaging.IMessage responseMsg, out ITransportHeaders responseHeaders, out System.IO.Stream responseStream)
        {
            if( requestMsg == null )
                throw new ArgumentNullException("requestMsg");
            LogicalCallContext context = (LogicalCallContext)requestMsg.Properties["__CallContext"];
            string hostName = (string)context.GetData("HostName");
            log4net.ThreadContext.Properties["ClientHostName"] = hostName;
            ServerContext.Current = new ServerContext { ClientHostName = hostName };

            var result = _nextChannelSink.ProcessMessage(sinkStack, requestMsg, requestHeaders, requestStream, out responseMsg, out responseHeaders, out responseStream);
            ServerContext.Current = null;
            return result;
        }

        #endregion
    }
}
