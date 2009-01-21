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
    //<Extension()> _
    //Public Function AddScript(ByVal master As MasterPage, ByVal src As String, Optional ByVal type As String = "text/javascript") As HtmlGenericControl
    //    Dim script As New System.Web.UI.HtmlControls.HtmlGenericControl("script")
    //    script.Attributes("type") = type
    //    script.Attributes("src") = src
    //    master.Page.Header.Controls.Add(script)
    //    Return script
    //End Function

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
}
