// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class PartitionFileStream : Stream
    {
        private Stream _baseStream;
        private readonly string _fileName;
        private readonly IEnumerator<PartitionFileIndexEntry> _indexEntries;
        private PartitionFileIndexEntry _current;
        private ChecksumInputStream _currentSegment;
        private readonly long _length;
        private long _position;
        private readonly int _bufferSize;

        public PartitionFileStream(string fileName, int bufferSize, IEnumerable<PartitionFileIndexEntry> indexEntries)
        {
            _fileName = fileName;
            _bufferSize = bufferSize;
            int segmentCount = indexEntries.Count();
            _length = indexEntries.Sum(e => e.Count) - segmentCount;
            _indexEntries = indexEntries.GetEnumerator();
            _baseStream = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize);
            if( NextSegment() )
            {
                if( _currentSegment.IsChecksumEnabled )
                    _length -= (sizeof(uint) * segmentCount);
            }
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get { return _length; }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > buffer.Length )
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if( _currentSegment == null )
                return 0;

            int totalBytesRead = 0;
            while( count > 0 )
            {
                int bytesRead = _currentSegment.Read(buffer, offset, count);
                if( bytesRead == 0 && !NextSegment() )
                    break;
                totalBytesRead += bytesRead;
                _position += bytesRead;
                count -= bytesRead;
                offset += bytesRead;
            }

            return totalBytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( disposing )
            {
                _indexEntries.Dispose();
                if( _currentSegment != null )
                    _currentSegment.Dispose();
                if( _baseStream != null )
                    _baseStream.Dispose();
            }
        }

        private bool NextSegment()
        {
            if( _currentSegment != null )
                _currentSegment.Dispose();

            if( !_indexEntries.MoveNext() )
            {
                _currentSegment = null;
                return false;
            }

            _current = _indexEntries.Current;
            _baseStream.Seek(_current.Offset, SeekOrigin.Begin);
            _currentSegment = new ChecksumInputStream(_baseStream, false, _current.Count);
            return true;
        }
    }
}
