// AJAX support script v1.1. Copyright (c) 2006, Sven Groot
// 2006-12-31

function tryCreateActiveXObject(progids)
{
	for( x = 0; x < progids.length; ++x )
	{
		try
		{
			var result = new ActiveXObject(progids[x]);
			if( result != null )
			{
				return result;	
			}
		}
		catch( E )
		{
			// ignore
		}
	}
	
	throw 'No ActiveX object could be created for the specified progids';
}

function installXMLHTTPRequest()
{
    if( !window.XMLHttpRequest )
    {
        window.XMLHttpRequest = function()
        {
            return tryCreateActiveXObject(["Msxml2.XMLHTTP.6.0", "Msxml2.XMLHTTP.3.0", "Msxml2.XMLHTTP", "Microsoft.XMLHTTP"]);
        }
    }
}

installXMLHTTPRequest();

function createCallback(method, context)
{
    return function() { method(context) }
}

function Request(method, url, oncomplete, onerror)
{
    this.requestComplete = function(context)
    {
        if( context._xmlhttp.readyState == 4 )
        {
            if( context._xmlhttp.status == 200 )
            {
                context._oncomplete(context._xmlhttp.responseXML);
            }
            else if( context._xmlhttp.status != 0 ) // 0 means abort
            {
                if( context._onerror )
                    context._onerror(context._xmlhttp.status, context._xmlhttp.statusText, context._xmlhttp.responseXML);
                else
                    alert("The server returned an unexpected response: " + context._xmlhttp.status + " " + context._xmlhttp.statusText);
            }
        }
    }
	this._oncomplete = oncomplete;
	this._onerror = onerror;
    this._xmlhttp = new XMLHttpRequest();
	this._xmlhttp.open(method, url, true);
	this._xmlhttp.onreadystatechange = createCallback(this.requestComplete, this);
}

Request.prototype.setMimeType = function(mimeType) 
{
    this._xmlhttp.setRequestHeader("Content-Type", mimeType);
}

Request.prototype.send = function(data)
{
    this._xmlhttp.send(data);
}

// Simple SOAP Web Service support.
function WebMethod(url, namespaceURI, method, types, oncomplete, onerror)
{
    this._req = new Request("POST", url, webMethodComplete, webMethodError);
    this._oncomplete = oncomplete;
    if( onerror )
		this._onerror = onerror;
	else
		this._onerror = function(msg) { alert(msg); }
    this._method = method;
    this._namespaceURI = namespaceURI;
    this._types = types;
    this._req._webMethod = this;
}

WebMethod.prototype.call = function(params)
{
    this._req.setMimeType("text/xml; charset=utf-8");
    this._req._xmlhttp.setRequestHeader("SOAPAction", "\"" + this._namespaceURI + "/" + this._method + "\"");
    var body = buildRequest(this._namespaceURI, this._method, params);
    this._req.send(body);    
}

WebMethod.prototype.abort = function()
{
    this._req._xmlhttp.abort();
}

function buildRequest(namespaceURI, method, params)
{
    var request = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                    + "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                    + "<soap:Body>"
                    + "<" + method + " xmlns=\"" + namespaceURI + "\">";
    
    if( params )
        request += params.SoapSerialize();
        
    request += "</" + method + ">"
            + "</soap:Body>"
            + "</soap:Envelope>";
            
    
    return request;
}

function xmlEncode(text)
{
	return text.replace(/&/g, "&amp;").replace(/\</g, "&lt;").replace(/\>/g, "&gt;");
}

Array.prototype.indexOf = function(value)
{
    for( var x = 0; x < this.length; ++x )
    {
        if( this[x] == value )
            return x;
    }
    
    return -1;
}

function Deserialize(elt, types)
{
    // Three simple rules
    // One child which is text: treat as string
    // Multiple children, different names: complex type
    // Multiple children, same names: array
    // One child, depends on known type or not.
    if( elt.childNodes.length == 1 && elt.firstChild.nodeType == 3 ) // text node
        return elt.firstChild.nodeValue;
    else
    {
        // Selects all elements
        var elts = simpleSelectNodes(elt, "*");
        var result = null;
        if( elts.length > 1 )
        {
            if( getLocalName(elts[0]) == getLocalName(elts[1]) )
            {
                // Array
                result = new Array();
                for( var x = 0; x < elts.length; ++x )
                {
                    var obj = Deserialize(elts[x], types);
                    obj.SoapTypeName = getLocalName(elts[x]);
                    result.push(obj);
                }
            }
            else
            {
                result = new CompoundObject();
                for( var x = 0; x < elts.length; ++x )
                    result[getLocalName(elts[x])] = Deserialize(elts[x], types);
            }
        }
        else if( elts.length == 1 )
        {
            if( types != null && types.indexOf(getLocalName(elts[0])) != -1 )
            {
                result = new Array();
                var obj = Deserialize(elts[0], types);
                obj.SoapTypeName = getLocalName(elts[0]);
                result.push(obj);
            }
            else
            {
                result = new CompoundObject();
                result[getLocalName(elts[0])] = Deserialize(elts[0], types);
            }
        }
        return result;
    }
}

function createWebServiceNSResolver(namespaceURI)
{
    return function(prefix)
    {
        if( prefix == "soap" )
            return "http://schemas.xmlsoap.org/soap/envelope/";
        else if( prefix == "s" )
            return namespaceURI;
        else
            return null;
    }
}

function webMethodComplete(doc)
{
    var NSResolver = createWebServiceNSResolver(this._webMethod._namespaceURI);
    var response = simpleSelectSingleNode(doc, "/soap:Envelope/soap:Body/s:" + this._webMethod._method + "Response", NSResolver);
    if( response == null )
    {
		var msg = "Invalid response received from the server.";
		this._webMethod._onerror(msg);
    }
    else
    {
        var result = null;
        var additional = null;
        var resultElements = simpleSelectNodes(response, "s:*", NSResolver);
        for( var x = 0; x < resultElements.length; ++x )
        {
            var resultElt = resultElements[x];
            var localName = getLocalName(resultElt)
            if( localName == this._webMethod._method + "Result" )
            {
                result = Deserialize(resultElt, this._webMethod._types);
            }
            else
            {
                if( additional == null )
                    additional = new Object();
                additional[localName] = Deserialize(resultElt, this._webMethod._types);
            }    
        }
        this._webMethod._oncomplete(result, additional);
    }
}

function webMethodError(status, statusText, doc)
{
    var resolver = createWebServiceNSResolver(this._webMethod._namespaceURI);
    var friendlyMessageNode = simpleSelectSingleNode(doc, "/soap:Envelope/soap:Body/soap:Fault/detail/@friendlyMessage", resolver);
    var faultstring = simpleSelectSingleNode(doc, "/soap:Envelope/soap:Body/soap:Fault/faultstring", resolver);
    var msg;
    if( faultstring != null && faultstring.firstChild != null && faultstring.firstChild.nodeType == 3 )
		msg = faultstring.firstChild.nodeValue;
	else
		msg = status + " " + statusText;
		
	var friendlyMessage = null;
	if( friendlyMessageNode != null )
	    friendlyMessage = friendlyMessageNode.nodeValue;
		
	this._webMethod._onerror(msg, friendlyMessage);
}

// Helper type for compound type serialization

function CompoundObject()
{
}

CompoundObject.prototype.SoapSerialize = function()
{
    var result = "";
    for( var prop in this )
    {
        if( typeof(this[prop]) != "function" && this[prop] != null )
            result += "<" + prop + ">" + this[prop].SoapSerialize() + "</" + prop + ">";
    }
    return result;
}

// Default SOAP Serialization rules for types.

Object.prototype.SoapSerialize = function()
{
    return xmlEncode(this.toString());
}

Date.prototype.SoapSerialize = function()
{
    return this.getFullYear() + "-" + FormatNumberForSerialization(this.getMonth()+1) + "-" + FormatNumberForSerialization(this.getDate())
        + "T" + FormatNumberForSerialization(this.getHours()) + ":" + FormatNumberForSerialization(this.getMinutes()) + ":" + FormatNumberForSerialization(this.getSeconds());
}

Array.prototype.SoapSerialize = function()
{
    var result = "";
    for( var x = 0; x < this.length; ++x )
    {
        var obj = this[x];
        result += "<" + obj.SoapTypeName + ">" + obj.SoapSerialize() + "</" + obj.SoapTypeName + ">";
    }
    
    return result;
}

function FormatNumberForSerialization(number)
{
    return number < 10 ? "0" + number : number;
}

// Utility methods

Date.parseXSD = function(string)
{
    var dateTimeParts = string.split("T");
    if( dateTimeParts.length != 2 )
        return null;
    
    var dateParts = dateTimeParts[0].split("-");
    if( dateParts.length != 3 )
        return null;
    
    var timeParts = dateTimeParts[1].split(":");
    if( timeParts.length != 3 )
        return null;
    
    var seconds;
    var milliseconds = 0;
    if( timeParts[2].indexOf(".") != -1 )
    {
        var secondParts = timeParts[2].split(".");
        if( secondParts.length != 2 )
            return null;
        seconds = secondParts[0];
        milliseconds = secondParts[1];
    }
    else
    {
        seconds = timeParts[2];
    }
    
    return new Date(dateParts[0], dateParts[1] - 1, dateParts[2], timeParts[0], timeParts[1], seconds, milliseconds);
}