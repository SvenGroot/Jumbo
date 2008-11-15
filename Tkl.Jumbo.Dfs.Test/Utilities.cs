using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Test
{
    static class Utilities
    {
        private static readonly string _testOutputPath = GetOutputPath();

        private static string GetOutputPath()
        {
            string path = Environment.GetEnvironmentVariable("JUMBO_TESTOUTPUT");
            if( string.IsNullOrEmpty(path) )
                path = System.IO.Path.Combine(Environment.CurrentDirectory, "TestOutput");
            return path;
        }

        public static string TestOutputPath
        {
            get { return _testOutputPath; }
        }

        public static string GenerateFile(string name, int size)
        {
            string path = System.IO.Path.Combine(TestOutputPath, name);
            using( FileStream stream = System.IO.File.Create(path) )
            {
                GenerateData(stream, size);
            }
            return path;
        }

        public static void GenerateData(Stream stream, int size)
        {
            Random rnd = new Random();
            int sizeRemaining = size;
            byte[] buffer = new byte[4096];
            while( sizeRemaining > 0 )
            {
                int writeSize = Math.Min(buffer.Length, sizeRemaining);
                rnd.NextBytes(buffer);
                stream.Write(buffer, 0, writeSize);
                sizeRemaining -= writeSize;
            }
        }

        public static Packet GeneratePacket(int size, bool isLastPacket)
        {
            Random rnd = new Random();
            byte[] data = new byte[size];
            rnd.NextBytes(data);
            return new Packet(data, size, isLastPacket);
        }

        public static void CopyStream(Stream src, Stream dest)
        {
            byte[] buffer = new byte[4096];
            int bytesRead = 0;
            do
            {
                bytesRead = src.Read(buffer, 0, buffer.Length);
                if( bytesRead > 0 )
                {
                    dest.Write(buffer, 0, bytesRead);
                }
            } while( bytesRead > 0 );
        }

        public static bool CompareStream(Stream stream1, Stream stream2)
        {
            byte[] buffer1 = new byte[4096];
            byte[] buffer2 = new byte[4096];
            int bytesRead1 = 0;
            int bytesRead2 = 0;
            do
            {
                bytesRead1 = stream1.Read(buffer1, 0, buffer1.Length);
                bytesRead2 = stream2.Read(buffer2, 0, buffer2.Length);
                if( bytesRead1 > 0 && bytesRead1 == bytesRead2 )
                {
                    if( !CompareArray(buffer1, 0, buffer2, 0, bytesRead1) )
                        return false;
                }
            } while( bytesRead1 > 0 && bytesRead1 == bytesRead2 );
            return bytesRead1 == bytesRead2;
        }

        public static bool CompareArray<T>(T[] array1, int offset1, T[] array2, int offset2, int count)
        {
            int end = offset1 + count;
            for( int i1 = offset1, i2 = offset2; i1 < end; ++i1, ++i2 )
            {
                if( !object.Equals(array1[i1], array2[i2] ) )
                    return false;
            }
            return true;
        }

        public static void TraceLineAndFlush(string message)
        {
            Trace.WriteLine(message);
            Trace.Flush();
        }
    }
}
