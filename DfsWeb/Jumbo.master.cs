using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;

public partial class Jumbo : System.Web.UI.MasterPage
{
    protected void Page_Load(object sender, EventArgs e)
    {
        JumboVersionLabel.Text = typeof(DfsClient).Assembly.GetName().Version.ToString();
        OsVersionLabel.Text = Environment.OSVersion.ToString();
        ClrVersionLabel.Text = RuntimeEnvironment.Description;
        ArchitectureLabel.Text = (IntPtr.Size * 8).ToString();
    }
}
