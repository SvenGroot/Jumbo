using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using System.IO;

public partial class _Default : System.Web.UI.Page 
{
    protected void Page_Load(object sender, EventArgs e)
    {
        DfsClient client = new DfsClient();
        DfsMetrics metrics = client.NameServer.GetMetrics();
        using( StringWriter writer = new StringWriter() )
        {
            metrics.PrintMetrics(writer);
            Status.InnerText = writer.ToString();
        }
    }
}
