// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI.HtmlControls;
using System.Web.UI;

/// <summary>
/// Summary description for Extensions
/// </summary>
public static class Extensions
{
    public static HtmlGenericControl AddScript(this MasterPage master, string src)
    {
        if( master == null )
            throw new ArgumentNullException("master");
        if( src == null )
            throw new ArgumentNullException("src");

        HtmlGenericControl script = new HtmlGenericControl("script");
        script.Attributes["type"] = "text/javascript";
        script.Attributes["src"] = src;
        master.Page.Header.Controls.Add(script);
        return script;
    }

    public static HtmlLink AddStyleSheet(this MasterPage master, string href)
    {
        if( master == null )
            throw new ArgumentNullException("master");
        if( href == null )
            throw new ArgumentNullException("src");

        HtmlLink link = new HtmlLink();
        link.Attributes["rel"] = "stylesheet";
        link.Attributes["type"] = "text/css";
        link.Href = href;
        master.Page.Header.Controls.Add(link);
        return link;
    }
}
