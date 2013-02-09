// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

public partial class removedataserver : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
    }

    protected void _removeButton_Click(object sender, EventArgs e)
    {
        string dataServer = Request.QueryString["dataServer"];
        int port = Convert.ToInt32(Request.QueryString["port"]);

        DfsClient client = (DfsClient)FileSystemClient.Create();
        client.NameServer.RemoveDataServer(new Ookii.Jumbo.ServerAddress(dataServer, port));
        Response.Redirect("Default.aspx");
    }
}
