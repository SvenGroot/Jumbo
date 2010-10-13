// $Id$
//
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
        DfsClient client = new DfsClient();
        Guid[] blocks;
        string newQueryString;
        if( dataServer == null )
        {
            BlockKind kind = BlockKind.Normal;
            string kindString = Request.QueryString["kind"];
            if( kindString != null )
            {
                kind = (BlockKind)Enum.Parse(typeof(BlockKind), kindString, true);
            }
            blocks = client.NameServer.GetBlocks(kind);
            newQueryString = "kind=" + kind.ToString();
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            ServerAddress address = new Tkl.Jumbo.ServerAddress(dataServer, port);
            _pageTitle.InnerText = string.Format("Block list for {0}", address);
            Title = string.Format("Block list for {0} - Jumbo DFS", address);
            blocks = client.NameServer.GetDataServerBlocks(address);
            newQueryString = string.Format("dataServer={0}&port={1}", dataServer, port);
        }

        if( blocks != null )
        {
            bool includeFiles = Request.QueryString["files"] == "true";
            if( includeFiles )
            {
                ShowFilesLink.InnerText = "Hide files.";
                ShowFilesLink.HRef = "blocklist.aspx?" + newQueryString;
            }
            else
            {
                ShowFilesLink.InnerText = "Show files.";
                ShowFilesLink.HRef = "blocklist.aspx?" + newQueryString + "&files=true";
            }

            foreach( Guid blockId in blocks )
            {
                HtmlGenericControl li = new HtmlGenericControl("li");
                if( includeFiles )
                    li.InnerText = string.Format("{0:B}: {1}", blockId, client.NameServer.GetFileForBlock(blockId));
                else
                    li.InnerText = blockId.ToString("B");
                _blockList.Controls.Add(li);
            }
        }
    }
}
