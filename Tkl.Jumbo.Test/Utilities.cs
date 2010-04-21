using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Test.Tasks;

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

        public static int GenerateDataLines(Stream stream, int size)
        {
            Random rnd = new Random();
            int sizeRemaining = size;
            int lines = 0;
            while( sizeRemaining > 0 )
            {
                stream.WriteByte((byte)rnd.Next('a', 'z'));
                if( sizeRemaining % 80 == 0 )
                {
                    stream.WriteByte((byte)'\n');
                    --sizeRemaining;
                    ++lines;
                }
                --sizeRemaining;
            }
            return lines + 1;
        }

        public static List<string> GenerateTextData(int length, int count)
        {
            List<string> result = new List<string>(count);
            StringBuilder sb = new StringBuilder(length);
            Random rnd = new Random();
            for( int x = 0; x < count; ++x )
            {
                sb.Length = 0;
                for( int l = 0; l < length; ++l )
                {
                    sb.Append((char)rnd.Next('a', 'z'));
                }
                result.Add(sb.ToString());
            }
            return result;
        }

        public static byte[] GenerateData(int size)
        {
            Random rnd = new Random();
            byte[] data = new byte[size];
            rnd.NextBytes(data);
            return data;
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
            CopyStream(src, dest, 4096);
        }

        public static void CopyStream(Stream src, Stream dest, int bufferSize)
        {
            byte[] buffer = new byte[bufferSize];
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
            return CompareStream(stream1, stream2, 4096);
        }

        public static bool CompareStream(Stream stream1, Stream stream2, int bufferSize)
        {
            byte[] buffer1 = new byte[bufferSize];
            byte[] buffer2 = new byte[bufferSize];
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
            return CompareList(array1, offset1, array2, offset2, count);
        }

        public static bool CompareList<T>(IList<T> list1, int offset1, IList<T> list2, int offset2, int count)
        {
            int end = offset1 + count;
            for( int i1 = offset1, i2 = offset2; i1 < end; ++i1, ++i2 )
            {
                if( !object.Equals(list1[i1], list2[i2]) )
                    return false;
            }
            return true;
        }

        public static bool CompareList<T>(IList<T> list1, IList<T> list2)
        {
            if( list1.Count != list2.Count )
                return false;
            else
                return CompareList(list1, 0, list2, 0, list1.Count);
        }

        public static void TraceLineAndFlush(string message)
        {
            Trace.WriteLine(message);
            Trace.Flush();
        }

        public static void GenerateJoinData(IList<Customer> customers, IList<Order> orders, int customerCount, int perCustomerRecordMax, int ordersPerCustomerMax)
        {
            Random rnd = new Random();
            string[] words = File.ReadAllLines("english-words.10");
            int orderId = 0;

            for( int x = 1; x <= customerCount; ++x )
            {
                int records = rnd.Next(1, perCustomerRecordMax);
                for( int y = 0; y < records; ++y )
                {
                    customers.Add(new Customer() { Id = x, Name = words[rnd.Next(words.Length)] });
                }
                int orderCount = rnd.Next(0, ordersPerCustomerMax + 1);
                for( int y = 0; y < orderCount; ++y )
                {
                    orders.Add(new Order() { Id = ++orderId, CustomerId = x, ItemId = rnd.Next(100) });
                }
            }
        }
    }
}
