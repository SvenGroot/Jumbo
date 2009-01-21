/// <reference path="simplexpath.js" />
/// <reference path="ajax.js" />
/// <reference path="number-functions.js" />

window.onload = function()
{
    var root = document.getElementById("FileSystemRoot");
    root.DfsFullPath = "/";
    callGetComments("/", root);
}

function callGetComments(path, listItem)
{
    var types = new Array();
    types.push("FileSystemEntryInfo");
    var m = new WebMethod("FileSystemService.asmx", "http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/FileSystemService", "GetDirectoryContents", types, getDirectoryContentsComplete, serviceError);
    m.listItem = listItem;
    var params = new CompoundObject();
    params.path = path;
    m.call(params);

    return m;
}

function loadDirectory(src)
{
    var li = src.parentNode.parentNode;
    if( li.childNodes.length == 1 )
        callGetComments(li.DfsFullPath, li);
    else
        li.removeChild(li.childNodes[1]);
}

function refreshDirectory(directory, listItem)
{
    var ul = createHtmlElement("ul");
    if( directory.Children && directory.Children.length > 0 )
    {
        for( var x = 0; x < directory.Children.length; ++x )
        {
            var item = directory.Children[x];
            var li = createHtmlElement("li");
            var span = createHtmlElement("span");
            var img = createHtmlElement("img");
            span.appendChild(img);
            var date = Date.parseXSD(item.DateCreated);
            span.appendChild(document.createTextNode(item.Name + " (" + date.ToUtcString()));
            if( item.IsDirectory == "true" )
            {                
                img.src = "images/folder_open.png";
                img.onclick = function() { loadDirectory(this) };
                img.className = "directory";
            }
            else
            {
                span.appendChild(document.createTextNode("; "));
                span.appendChild(formatSize(new Number(item.Size)));
                img.src = "images/generic_document.png";
            }
            span.appendChild(document.createTextNode(")"));
            img.alt = item.FullPath;
            img.title = item.FullPath;
            li.appendChild(span);
            li.DfsFullPath = item.FullPath;
            ul.appendChild(li);
        }
    }
    else
    {
        var li = createHtmlElement("li");
        li.appendChild(document.createTextNode("<empty>"));
        ul.appendChild(li);
    }
    listItem.appendChild(ul);
}

function getDirectoryContentsComplete(result)
{
    refreshDirectory(result, this.listItem);
}

function serviceError(msg)
{
    alert(msg);
}

function createHtmlElement(name)
{
	if( typeof(document.createElementNS) == "function" )
		return document.createElementNS("http://www.w3.org/1999/xhtml", name);
	else
		return document.createElement(name);
}

function formatSize(bytes)
{
    var size;
    var unit;
    if( bytes > 0x40000000 )
    {
        size = bytes / 0x40000000;
        unit = "GB";
    }
    else if( bytes > 0x100000 )
    {
        size = bytes / 0x100000;
        unit = "MB";
    }
    else if( bytes > 0x400 )
    {
        size = bytes / 0x400;
        unit = "KB";
    }
    else
    {
        return document.createTextNode(bytes.numberFormat("#,0") + " bytes");
    }
    var abbr = createHtmlElement("abbr");
    abbr.title = bytes.numberFormat("#,0") + " bytes";
    abbr.appendChild(document.createTextNode(size.numberFormat("#,0.0") + " " + unit));
    return abbr;
}

Date.prototype.ToUtcString = function()
{
    return this.getFullYear() + "-" + FormatNumberForSerialization(this.getMonth()+1) + "-" + FormatNumberForSerialization(this.getDate())
        + " " + FormatNumberForSerialization(this.getHours()) + ":" + FormatNumberForSerialization(this.getMinutes()) + ":" + FormatNumberForSerialization(this.getSeconds()) + "Z";
}