// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Messaging;

namespace Tkl.Jumbo
{
    /// <summary>
    /// .Net Remoting channel sink responsible for adding the host name to the calling context
    /// when calling a remote method.
    /// </summary>
    class ClientChannelSink : BaseChannelObjectWithProperties, IMessageSink, IClientChannelSink
    {
        private readonly IMessageSink _nextMessageSink;
        private readonly IClientChannelSink _nextClientChannelSink;

        public ClientChannelSink(object next)
        {
            _nextMessageSink = next as IMessageSink;
            _nextClientChannelSink = next as IClientChannelSink;
        }

        #region IMessageSink Members

        public IMessageCtrl AsyncProcessMessage(IMessage msg, IMessageSink replySink)
        {
            AddHostNameToMessage(msg);
            return _nextMessageSink.AsyncProcessMessage(msg, replySink);
        }

        public IMessageSink NextSink
        {
            get { return _nextMessageSink; }
        }

        public IMessage SyncProcessMessage(IMessage msg)
        {
            AddHostNameToMessage(msg);
            return _nextMessageSink.SyncProcessMessage(msg);
        }

        #endregion

        #region IClientChannelSink Members

        public void AsyncProcessRequest(IClientChannelSinkStack sinkStack, IMessage msg, ITransportHeaders headers, System.IO.Stream stream)
        {
            throw new NotImplementedException();
        }

        public void AsyncProcessResponse(IClientResponseChannelSinkStack sinkStack, object state, ITransportHeaders headers, System.IO.Stream stream)
        {
            throw new NotImplementedException();
        }

        public System.IO.Stream GetRequestStream(IMessage msg, ITransportHeaders headers)
        {
            throw new NotImplementedException();
        }

        public IClientChannelSink NextChannelSink
        {
            get { return _nextClientChannelSink; }
        }

        public void ProcessMessage(IMessage msg, ITransportHeaders requestHeaders, System.IO.Stream requestStream, out ITransportHeaders responseHeaders, out System.IO.Stream responseStream)
        {
            throw new NotImplementedException();
        }

        #endregion

        private static void AddHostNameToMessage(IMessage msg)
        {
            LogicalCallContext context = (LogicalCallContext)msg.Properties["__CallContext"];
            context.SetData("HostName", ServerContext.LocalHostName);
        }
    }
}
