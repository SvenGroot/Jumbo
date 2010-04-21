using System;
using System.Text;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Provides extension methods for the <see cref="String"/> class.
    /// </summary>
    /// <threadsafety static="true" instance="false"/>
    public static class StringExtensions
    {
        /// <summary>
        /// Breaks a string into lines of a specified maximum length, breaking on word boundaries when possible.
        /// </summary>
        /// <param name="text">The string to break up into lines.</param>
        /// <param name="maxLineLength">The maximum length, in characters, of each line.</param>
        /// <param name="indent">The number of characters by which to indent all lines except the first. </param>
        /// <returns>The text, split into lines.</returns>
        /// <remarks>
        /// <para>
        ///   When using this function to format text for display on the console, use <see cref="Console.WindowWidth"/> - 1
        ///   as the value for <paramref name="maxLineLength"/>. If you don't subtract 1, this can lead to blank lines in
        ///   case a line is exactly the maximum width.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="text"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxLineLength"/> is not greater than zero, or <paramref name="indent"/> is less than zero.</exception>
        /// <exception cref="ArgumentException"><paramref name="maxLineLength"/> is not greater than <paramref name="indent"/>.</exception>
        public static string SplitLines(this string text, int maxLineLength, int indent)
        {
            if( text == null )
                throw new ArgumentNullException("text");
            if( maxLineLength <= 0 )
                throw new ArgumentOutOfRangeException("maxLineLength", Properties.Resources.MaxLineLengthOutOfRange);
            if( indent < 0 )
                throw new ArgumentOutOfRangeException("indent", Properties.Resources.IndentOutOfRange);
            if( indent >= maxLineLength )
                throw new ArgumentException(Properties.Resources.MaxLineLengthSmallerThanIndent);

            // I'm aware that there are probably much faster ways to do this, but as this is intended to print usage information
            // on the console it's hardly a performance-critical part so not worth the effort.
            StringBuilder lines = new StringBuilder(text.Length);
            int lineStart = 0;
            int lastBreakChar = 0;
            string padding = new string(' ', indent);
            for( int x = 0; x < text.Length; ++x )
            {
                char ch = text[x];
                if( ch == '\r' || ch == '\n' )
                {
                    // Leave existing line breaks in the text.
                    if( lineStart > 0 )
                        lines.Append(padding);
                    lines.AppendLine(text.Substring(lineStart, x - lineStart));
                    if( ch == '\r' && x + 1 < text.Length && text[x + 1] == '\n' ) // Handle Windows line endings.
                        ++x;
                    lineStart = x + 1;
                }
                else if( char.IsWhiteSpace(ch) )
                {
                    // Record the location of the last place where we could break the line.
                    lastBreakChar = x;
                }

                // Check if we must break the line.
                int lineLength = x - lineStart;
                if( lineStart > 0 ) // not the first line?
                    lineLength += indent;
                if( lineLength >= maxLineLength )
                {
                    if( lineStart > 0 )
                        lines.Append(padding);
                    if( lastBreakChar <= lineStart )
                    {
                        // No place to break the line, just break the width
                        lines.AppendLine(text.Substring(lineStart, x - lineStart));
                        lineStart = x;
                    }
                    else
                    {
                        // Break at the last occurrence of whitespace.
                        lines.AppendLine(text.Substring(lineStart, lastBreakChar - lineStart));
                        lineStart = lastBreakChar + 1;
                    }
                }
            }

            if( lineStart < text.Length )
            {
                if( lineStart > 0 )
                    lines.Append(padding);
                lines.AppendLine(text.Substring(lineStart));
            }

            return lines.ToString();
        }
    }
}
