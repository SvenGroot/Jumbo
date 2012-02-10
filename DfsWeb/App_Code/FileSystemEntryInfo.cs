// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

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
        DateCreated = entry.DateCreated;
        FullPath = entry.FullPath;
        JumboFile file = entry as JumboFile;
        if( file != null )
        {
            Size = file.Size;
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

    public long Size { get; set; }

    public DateTime DateCreated { get; set; }

    public FileSystemEntryInfo[] Children { get; set; }
}
