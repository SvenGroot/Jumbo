/// <reference path="jquery-1.6.4-vsdoc.js" />
/// <reference path="json2.js" />

$(document).ready(function()
{
    $("#FileSystemRoot > img").click(toggleDirectory);
    getDirectoryListing("/", $("#FileSystemRoot"));
    $("input").focus(function() { $(this).select(); });
});

function getDirectoryListing(path, li)
{
    jsonAjax("FileSystemService.asmx", "GetDirectoryListing", { "path": path }, function(result) {
            var ul = $("<ul>");
            $(li).children("span").children("img").attr("src", "images/arrow_open.png");
            if( result.d.Children && result.d.Children.length > 0 )
            {
                $.each(result.d.Children, function(index, child)
                {
                    var img = $("<img>").attr("alt", child.FullPath).attr("title", child.FullPath);
                    var outerSpan = $("<span>")
                    var span = $("<span>").addClass("fileSystemEntry").append(img).appendTo(outerSpan);
                    span.append(document.createTextNode(child.Name + " (" + child.DateCreated));
                    if( child.IsDirectory )
                    {
                        img.attr("src", "images/folder_open.png");
                        $("<img>").attr("src", "images/arrow_closed.png").attr("alt", "expand").click(toggleDirectory).addClass("directory").prependTo(outerSpan);
                    }
                    else
                    {
                        img.attr("src", "images/generic_document.png");
                        span.append(document.createTextNode("; "));
                        span.append($("<abbr>").attr("title", child.SizeInBytes + " bytes").text(child.FormattedSize));
                        span.css("margin-left", "21px");
                    }
                    span.append(document.createTextNode(")"));
                    var fullPath = child.FullPath;
                    span.click(function()
                    {
                        $("#EntryName").text(child.Name);
                        $("#EntryCreated").text(child.DateCreated);
                        $("#FullPath").attr("value", child.FullPath);
                        if( child.IsDirectory )
                            $("#FileInfo").hide();
                        else
                        {
                            $("#FileInfo").show();
                            $("#FileFormattedSize").text(child.FormattedSize);
                            $("#FileSize").text(child.SizeInBytes);
                            $("#BlockSize").text(child.BlockSize);
                            $("#BlockCount").text(child.BlockCount);
                            $("#ReplicationFactor").text(child.ReplicationFactor);
                            $("#RecordOptions").text(child.RecordOptions);
                            $("#BlockSize").text(child.BlockSize);
                            $("#ViewFileLink").attr("href", "viewfile.aspx?path=" + encodeURIComponent(child.FullPath) + "&maxSize=100KB&tail=false");
                            $("#DownloadLink").attr("href", "downloadfile.ashx?path=" + encodeURIComponent(child.FullPath));
                        }
                    });
//                    if( window.clipboardData )
//                    {
//                        var icons = $("<span>").addClass("icons").css("visibility", "hidden").appendTo(span);
//                        $("<img>").attr("src", "images/copy.png").attr("alt", "Copy path").attr("title", "Copy path").click(function() { window.clipboardData.setData("Text", fullPath) }).appendTo(icons);
//                        span.mouseenter(function() { $(".icons", this).css("visibility", "visible"); });
//                        span.mouseleave(function() { $(".icons", this).css("visibility", "hidden"); });
//                    }
                    ul.append($("<li>").append(outerSpan));
                });
            }
            ul.hide();
            $(li).append(ul);
            ul.show("normal");
        }, "Could not load directory contents.");
}

function toggleDirectory()
{
    var li = $(this).parent().parent();
    var listing = li.children("ul");
    if( listing.length > 0 )
    {
        $(this).attr("src", "images/arrow_closed.png");
        listing.hide("normal", function() { $(listing).remove(); });        
    }
    else
    {
        getDirectoryListing($(this).next("span").children("img").attr("alt"), li);
    }
}

function jsonAjax(url, method, params, success, errorMessage)
{
    return $.ajax({ 
        type: "POST",
        url: url + "/" + method,
        data: JSON.stringify(params),
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        async: true,
        cache: false,
        success: success,
        beforeSend: function(x, s) { x.params = params; },
        error: function(x, e) 
        { 
            if( e != "abort" )
            {
                if( x.responseText )
                    alert(errorMessage + ": " + $.parseJSON(x.responseText).Message);
                else
                    alert(errorMessage + ".");
            }
        }
    });
}