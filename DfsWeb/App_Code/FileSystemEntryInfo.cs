// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo;
using System.Globalization;

/// <summary>
/// Summary description for FileSystemEntryInfo
/// </summary>
public class FileSystemEntryInfo
{
    public FileSystemEntryInfo()
    {
    }

    public FileSystemEntryInfo(JumboFileSystemEntry entry, bool includeChildren)
    {
        Name = entry.Name;
        DateCreated = entry.DateCreated.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        FullPath = entry.FullPath;
        JumboFile file = entry as JumboFile;
        if( file != null )
        {
            SizeInBytes = file.Size.ToString("#,##0", CultureInfo.InvariantCulture);
            FormattedSize = new BinarySize(file.Size).ToString("SB", CultureInfo.InvariantCulture);
        }
        else
        {
            JumboDirectory dir = (JumboDirectory)entry;
            IsDirectory = true;
            if( includeChildren )
            {
                Children = (from child in dir.Children
                            orderby !(child is JumboDirectory), child.Name
                            select new FileSystemEntryInfo(child, false)).ToArray();
            }
        }
    }

    public string Name { get; set; }

    public string FullPath { get; set; }

    public bool IsDirectory { get; set; }

    public string SizeInBytes { get; set; }

    public string FormattedSize { get; set; }

    public string DateCreated { get; set; }

    public FileSystemEntryInfo[] Children { get; set; }
}
