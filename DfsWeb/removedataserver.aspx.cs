using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;

public partial class removedataserver : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
    }

    protected void _removeButton_Click(object sender, EventArgs e)
    {
        string dataServer = Request.QueryString["dataServer"];
        int port = Convert.ToInt32(Request.QueryString["port"]);

        DfsClient client = new DfsClient();
        client.NameServer.RemoveDataServer(new Tkl.Jumbo.ServerAddress(dataServer, port));
        Response.Redirect("Default.aspx");
    }
}
