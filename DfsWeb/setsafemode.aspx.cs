// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Dfs.FileSystem;

public partial class setsafemode : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        bool safeMode = Convert.ToBoolean(Request.QueryString["safeMode"]);
        _result.InnerText = string.Format("Turn safe mode {0}?", safeMode ? "ON" : "OFF");
    }

    protected void _safeModeButton_Click(object sender, EventArgs e)
    {
        bool safeMode = Convert.ToBoolean(Request.QueryString["safeMode"]);
        DfsClient client = (DfsClient)FileSystemClient.Create();
        try
        {
            client.NameServer.SafeMode = safeMode;
            Response.Redirect("Default.aspx");
        }
        catch( InvalidOperationException ex )
        {
            _result.InnerText = ex.Message;
        }
    }
}
