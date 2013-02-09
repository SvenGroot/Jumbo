// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

/// <summary>
/// Summary description for FileSystemService
/// </summary>
[WebService(Namespace = "http://www.ookii.iis.u-tokyo.ac.jp/schema/Jumbo/FileSystemService")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
// To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
[System.Web.Script.Services.ScriptService]
public class FileSystemService : System.Web.Services.WebService
{

    public FileSystemService()
    {

        //Uncomment the following line if using designed components 
        //InitializeComponent(); 
    }

    [WebMethod]
    public FileSystemEntryInfo GetDirectoryListing(string path)
    {
        FileSystemClient client = FileSystemClient.Create();
        return new FileSystemEntryInfo(client.GetDirectoryInfo(path), true);
    }

}

