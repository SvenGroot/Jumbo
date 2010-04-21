// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides extension methods for the <see cref="String"/> class.
    /// </summary>
    public static class StringExtensions
    {
        /// <summary>
        /// Breaks a string into lines of a certain maximum length.
        /// </summary>
        /// <param name="text">The string to break up into lines.</param>
        /// <param name="maxLineLength">The maximum length, in characters, of each line.</param>
        /// <param name="indent">The number of characters by which to indent all lines except the first. </param>
        /// <returns>The text, split into lines.</returns>
        public static string GetLines(this string text, int maxLineLength, int indent)
        {
            if( text == null )
                throw new ArgumentNullException("text");
            if( maxLineLength <= 0 )
                throw new ArgumentOutOfRangeException("maxLineLength", "The maximum line length must be greater than zero.");
            if( indent < 0 )
                throw new ArgumentOutOfRangeException("indent", "The indent must be greater than or equal to zero.");

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
                    if( lastBreakChar <= lineStart )
                    {
                        // No place to break the line, just break the width
                        if( lineStart > 0 )
                            lines.Append(padding);
                        lines.AppendLine(text.Substring(lineStart, x - lineStart));
                        lineStart = x;
                    }
                    else
                    {
                        // Break at the last occurrence of whitespace.
                        if( lineStart > 0 )
                            lines.Append(padding);
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
