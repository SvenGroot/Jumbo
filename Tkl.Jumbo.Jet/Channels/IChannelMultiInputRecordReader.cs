using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Interface used by multi input record readers that read data from a channel.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This interface can be used by multi input record readers that need to know what input stage
    ///   they are reading from.
    /// </para>
    /// </remarks>
    public interface IChannelMultiInputRecordReader
    {
        /// <summary>
        /// Gets or sets the input stage for the channel that this reader is reading from.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   This property will be set to the channel's input stage if this reader is reading from a channel.
        ///   If this reader is not reading from a channel (e.g. if it's the reader set in <see cref="StageConfiguration.MultiInputRecordReaderType"/>
        ///   which aggregates multiple channels, this property will be set to <see langword="null"/>.
        /// </para>
        /// </remarks>
        StageConfiguration InputStage { get; set; }
    }
}
