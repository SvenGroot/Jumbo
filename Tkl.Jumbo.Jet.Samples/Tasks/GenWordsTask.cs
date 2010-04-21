using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that generates input data for word count.
    /// </summary>
    public class GenWordsTask : Configurable, IPullTask<StringWritable, StringWritable>
    {
        #region Nested types

        private class WordInfo
        {
            public WordInfo(string word)
            {
                Word = word;
                Utf8ByteLength = Encoding.UTF8.GetByteCount(word);
            }

            public string Word { get; private set; }
            public int Utf8ByteLength { get; private set; }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenWordsTask));

        private const int _lineLength = 80;

        /// <summary>
        /// The name of the setting that specifies the directory holding the dictionary files in the <see cref="JobConfiguration.JobSettings"/> for the job.
        /// </summary>
        public const string DictionaryDirectorySetting = "DictionaryDirectory";
        /// <summary>
        /// The name of the setting that specifies the size, in bytes, of the data to generate in the <see cref="JobConfiguration.JobSettings"/> for the job.
        /// </summary>
        public const string SizePerTaskSetting = "SizePerTask";

        #region IPullTask<StringWritable,StringWritable> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<StringWritable> input, RecordWriter<StringWritable> output)
        {
            List<WordInfo> words = GetWordList();

            int size = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting(SizePerTaskSetting, 0);
            if( size <= 0 )
                throw new InvalidOperationException("No size specified or size is not larger than 0.");

            _log.InfoFormat("Words: {0}, Size: {1}", words.Count, size);

            Random rnd = new Random();

            int generated = 0;
            int lineLength = 0;
            int newLineLength = Environment.NewLine.Length;
            int prevGenerated = 0;
            StringBuilder line = new StringBuilder(_lineLength);
            StringWritable record = new StringWritable();
            while( generated + newLineLength < size )
            {
                prevGenerated = generated;
                WordInfo word = words[rnd.Next(words.Count)];
                generated += word.Utf8ByteLength + 1;
                lineLength += word.Utf8ByteLength + 1;
                if( generated + newLineLength < size )
                {
                    if( lineLength > _lineLength )
                    {
                        record.Value = line.ToString();
                        output.WriteRecord(record);
                        line.Length = 0;
                        generated += newLineLength;
                        lineLength = word.Utf8ByteLength + 1;
                    }
                    line.Append(word.Word);
                    line.Append(' ');
                }
            }
            if( line.Length > 0 )
            {
                record.Value = line.ToString();
                output.WriteRecord(record);
            }

            _log.InfoFormat("Generated {0} bytes of data.", prevGenerated + newLineLength);
        }

        private List<WordInfo> GetWordList()
        {
            DfsClient client = new DfsClient(DfsConfiguration);
            string dictionaryDirectoryName = TaskAttemptConfiguration.JobConfiguration.GetSetting(DictionaryDirectorySetting, null);
            DfsDirectory dictionaryDirectory = client.NameServer.GetDirectoryInfo(dictionaryDirectoryName);
            List<WordInfo> words = new List<WordInfo>();

            Encoding encoding = Encoding.GetEncoding("iso-8859-1");
            foreach( FileSystemEntry child in dictionaryDirectory.Children )
            {
                DfsFile file = child as DfsFile;
                if( file != null )
                {
                    using( DfsInputStream stream = client.OpenFile(file.FullPath) )
                    using( System.IO.StreamReader reader = new System.IO.StreamReader(stream, encoding) )
                    {
                        string word;
                        while( (word = reader.ReadLine()) != null )
                        {
                            if( word.Length > 0 )
                                words.Add(new WordInfo(word));
                        }
                    }
                }
            }

            if( words.Count == 0 )
                throw new InvalidOperationException("No words from which to generate input.");
            return words;
        }

        #endregion
    }
}
