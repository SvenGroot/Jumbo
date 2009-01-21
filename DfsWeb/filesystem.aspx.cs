using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class filesystem : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        Master.AddScript("script/simplexpath.js");
        Master.AddScript("script/ajax.js");
        Master.AddScript("script/filesystem.js");
        Master.AddScript("script/number-functions.js");
    }
}
