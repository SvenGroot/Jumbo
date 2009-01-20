<%@ Page Language="C#" AutoEventWireup="true"  CodeFile="Default.aspx.cs" Inherits="_Default" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head runat="server">
    <title>Jumbo DFS</title>
    <link rel="stylesheet" type="text/css" href="style/jumbo.css" />
</head>
<body>
    <div id="PageHeader">
        <p id="VersionInfo">
            Jumbo version: <asp:Label ID="JumboVersionLabel" runat="server" /><br />
            OS Version: <asp:Label ID="OsVersionLabel" runat="server" /><br />
            CLR Version: <asp:Label ID="ClrVersionLabel" runat="server" /> (<asp:Label ID="ArchitectureLabel" runat="server" /> bit runtime)
        </p>
        <h1>
            <em>Jumbo</em> DFS
        </h1>
    </div>
    <div id="MainContent">
        <h2>File system status</h2>
        <table>
            <tr>
                <th scope="row">Total size</th>
                <td id="TotalSizeColumn" runat="server"></td>
            </tr>
            <tr>
                <th scope="row">Total blocks (excluding pending)</th>
                <td id="BlocksColumn" runat="server"></td>
            </tr>
            <tr>
                <th scope="row">Under-replicated blocks</th>
                <td id="UnderReplicatedBlocksColumn" runat="server"></td>
            </tr>
            <tr>
                <th scope="row">Pending blocks</th>
                <td id="PendingBlocksColumn" runat="server"></td>
            </tr>
            <tr>
                <th scope="row">Data servers</th>
                <td id="DataServersColumn" runat="server"></td>
            </tr>
            <tr>
                <th scope="row">Safe mode</th>
                <td id="SafeModeColumn" runat="server"></td>
            </tr>
        </table>
        <h2>Data servers</h2>
        <table id="DataServerTable" runat="server">
            <tr>
                <th scope="col">Name</th>
                <th scope="col">Port</th>
                <th scope="col">Last contact</th>
                <th scope="col">Blocks</th>
                <th scope="col">Disk space</th>
            </tr>
        </table>
    </div>
</body>
</html>
