using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using System.Web.UI.HtmlControls;
using Tkl.Jumbo;

public partial class blocklist : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        string dataServer = Request.QueryString["dataServer"];
        int port = Convert.ToInt32(Request.QueryString["port"]);
        DfsClient client = new DfsClient();
        ServerAddress address = new Tkl.Jumbo.ServerAddress(dataServer, port);
        _pageTitle.InnerText = string.Format("Block list for {0}", address);
        Title = string.Format("Block list for {0} - Jumbo DFS", address);
        Guid[] blocks = client.NameServer.GetDataServerBlocks(address);

        foreach( Guid blockId in blocks )
        {
            HtmlGenericControl li = new HtmlGenericControl("li");
            li.InnerText = string.Format("{{{0}}}", blockId);
            _blockList.Controls.Add(li);
        }
    }
}
