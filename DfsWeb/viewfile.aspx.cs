using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo;
using System.Globalization;
using System.IO;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

public partial class viewfile : System.Web.UI.Page
{
    #region Nested types

    private class SizeLimitedStream : Stream
    {
        private readonly Stream _baseStream;
        private readonly long _size;

        public SizeLimitedStream(Stream baseStream, long size)
        {
            _baseStream = baseStream;
            _size = size;
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
            _baseStream.Flush();
        }

        public override long Length
        {
            get { return Math.Min(_size, _baseStream.Length); }
        }

        public override long Position
        {
            get
            {
                return _baseStream.Position;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if( Position + count > _size )
            {
                count = (int)(_size - Position);
                if( count < 0 )
                    count = 0;
            }


            return _baseStream.Read(buffer, offset, count);
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
    }


    #endregion

    protected void Page_Load(object sender, EventArgs e)
    {
        string path = Request.QueryString["path"];
        string maxSizeString = Request.QueryString["size"];
        long maxSize = maxSizeString == null ? 100 * BinarySize.Kilobyte : (long)BinarySize.Parse(maxSizeString, CultureInfo.InvariantCulture);
        bool tail = Request.QueryString["tail"] == "true";
        HeaderText.InnerText = string.Format(CultureInfo.InvariantCulture, "File '{0}' contents ({1} {2})", path, tail ? "last" : "first", new BinarySize(maxSize));
        try
        {
            FileSystemClient client = FileSystemClient.Create();
            using( Stream stream = client.OpenFile(path) )
            {
                if( tail )
                    stream.Position = Math.Max(0, stream.Length - maxSize);
                using( SizeLimitedStream sizeStream = new SizeLimitedStream(stream, tail ? stream.Length : maxSize) )
                using( StreamReader reader = new StreamReader(sizeStream) )
                {
                    FileContents.InnerText = reader.ReadToEnd();
                }
            }
        }
        catch( Exception ex )
        {
            FileContents.InnerText = ex.ToString();
            FileContents.Attributes["class"] = "error";
        }
    }
}