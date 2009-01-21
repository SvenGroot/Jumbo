/// <reference path="simplexpath.js" />
/// <reference path="ajax.js" />

window.onload = function()
{
    callGetComments("/");
}

function callGetComments(path)
{
    var types = new Array();
    types.push("FileSystemEntryInfo");
    var m = new WebMethod("FileSystemService.asmx", "http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/FileSystemService", "GetDirectoryContents", types, getDirectoryContentsComplete, serviceError);
    var params = new CompoundObject();
    params.path = path;
    m.call(params);

    return m;
}

function getDirectoryContentsComplete(result)
{
    alert(result.Children[0].Name);
}

function serviceError(msg)
{
    alert(msg);
}