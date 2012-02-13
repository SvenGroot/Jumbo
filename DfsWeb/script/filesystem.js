/// <reference path="jquery-1.6.4-vsdoc.js" />
/// <reference path="json2.js" />

$(document).ready(function()
{
    $("#FileSystemRoot img").click(toggleDirectory);
    getDirectoryListing("/", $("#FileSystemRoot"));
});

function getDirectoryListing(path, li)
{
    jsonAjax("FileSystemService.asmx", "GetDirectoryListing", { "path": path }, function(result) {
            var ul = $("<ul>");
            if( result.d.Children && result.d.Children.length > 0 )
            {
                $.each(result.d.Children, function(index, child)
                {
                    var img = $("<img>").attr("alt", child.FullPath).attr("title", child.FullPath);
                    var span = $("<span>").append(img);
                    span.append(document.createTextNode(child.Name + " (" + child.DateCreated));
                    if( child.IsDirectory )
                    {
                        img.attr("src", "images/folder_open.png");
                        img.click(toggleDirectory);
                        img.addClass("directory");
                    }
                    else
                    {
                        img.attr("src", "images/generic_document.png");
                        span.append(document.createTextNode("; "));
                        span.append($("<abbr>").attr("title", child.SizeInBytes + " bytes").text(child.FormattedSize));
                    }
                    span.append(document.createTextNode(")"));
                    var fullPath = child.FullPath;
                    if( window.clipboardData )
                    {
                        var icons = $("<span>").addClass("icons").css("visibility", "hidden").appendTo(span);
                        $("<img>").attr("src", "images/copy.png").attr("alt", "Copy path").attr("title", "Copy path").click(function() { window.clipboardData.setData("Text", fullPath) }).appendTo(icons);
                        span.mouseenter(function() { $(".icons", this).css("visibility", "visible"); });
                        span.mouseleave(function() { $(".icons", this).css("visibility", "hidden"); });
                    }
                    ul.append($("<li>").append(span));
                });
            }
            else
            {
                ul.append($("<li>").text("<empty>"));
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
        listing.hide("normal", function() { $(listing).remove(); });        
    }
    else
    {
        getDirectoryListing($(this).attr("alt"), li);
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