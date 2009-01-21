// Simple XPath script v1.0, copyright (c) 2006, Sven Groot
// This is far from a complete XPath implementation, just a slightly more robust way of selecting
// nodes than getElementsByTagName. It supports only location path expressions using only the child:: and
// attribute:: axes, and doesn't support predicates or functions. You must use abbreviated syntax (you can 
// say someNode and @someAtt but not actually child::someNode or attribute::someAtt), and you can only use 
// node name tests, not node type tests.

function XPathException(msg)
{
    this.message = msg;
}

function getLocalName(node)
{
    if( node.localName ) // W3C DOM
        return node.localName;
    else if( node.baseName ) // IE
        return node.baseName;
    else
        throw new XPathException("Insufficient DOM level for XPath.");
}

function compareNamespaceURI(uri1, uri2)
{
    return ((uri1 == null || uri1.length == 0) && (uri2 == null || uri2.length == 0)) || uri1 == uri2;
}

function resolveStep(node, step, NSResolver, result)
{
    var namespaceURI;
    var localName;
    var attribute = false;
    if( step.charAt(0) == "@" )
    {
        attribute = true;
        step = step.substr(1);
    }
    if( step == "*" )
    {
        namespaceURI = "*";
        localName = "*";
    }
    else
    {
        var nameParts = step.split(':');
        if( nameParts.length > 2 || nameParts.length < 1 )
            throw new XPathException("Invalid step syntax");
        namespaceURI = nameParts.length == 1 ? null : NSResolver(nameParts[0]);
        localName = nameParts.length == 1 ? nameParts[0] : nameParts[1];
    }
    var coll = attribute ? node.attributes : node.childNodes;
    var nodeType = attribute ? 2 : 1;
    for( x = 0; x < coll.length; ++x )
    {
        var childNode = coll[x];
        if( childNode.nodeType == nodeType && (localName == "*" || getLocalName(childNode) == localName) && (namespaceURI == "*" || compareNamespaceURI(childNode.namespaceURI, namespaceURI)) )
        {
            result.push(childNode);
        }
    }
}

function simpleSelectNodes(node, expr, NSResolver)
{
    var steps = expr.split('/');
    var step = 0;
    if( steps.length > 1 && steps[0].length == 0 ) // expr starts with a slash
    {
        if( node.ownerDocument != null )
            node = node.ownerDocument;
        step = 1;
    }
        
    var result = new Array();
    result.push(node);
    for( ; step < steps.length; ++step )
    {
        var newResult = new Array();
        for( x = 0; x < result.length; ++x )
        {
            resolveStep(result[x], steps[step], NSResolver, newResult);
        }
        result = newResult;
    }
    
    return result;
}

function simpleSelectSingleNode(node, expr, NSResolver)
{
    var result = simpleSelectNodes(node, expr, NSResolver);
    return result.length > 0 ? result[0] : null;
}
