// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;
using System.Globalization;

public partial class Jumbo : System.Web.UI.MasterPage
{
    protected void Page_Load(object sender, EventArgs e)
    {
        JumboVersionLabel.Text = string.Format(CultureInfo.CurrentCulture, "{0} ({1})", RuntimeEnvironment.JumboVersion, RuntimeEnvironment.JumboConfiguration);
        OsVersionLabel.Text = RuntimeEnvironment.OperatingSystemDescription;
        ClrVersionLabel.Text = RuntimeEnvironment.Description;
        ArchitectureLabel.Text = (IntPtr.Size * 8).ToString();
    }
}
