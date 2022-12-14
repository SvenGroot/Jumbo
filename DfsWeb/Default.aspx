<%@ Page Title="Jumbo DFS" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="Default.aspx.cs" Inherits="_Default" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2>File system status</h2>
    <p class="error" id="ErrorMessage" runat="server" visible="false"></p>
    <table>
        <tr>
            <th scope="row">Name server</th>
            <td id="NameServerColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Total size</th>
            <td id="TotalSizeColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Total capacity</th>
            <td id="TotalCapacityColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">DFS capacity used</th>
            <td id="DfsCapacityUsedColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Available capacity</th>
            <td id="AvailableCapacityColumn" runat="server"></td>
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
    <p>
        View name server log file: <a href="logfile.aspx?maxSize=100KB">last 100KB</a>, <a href="logfile.aspx?maxSize=0">all</a><br />
        <a href="filesystem.aspx">Browse file system namespace.</a>
    </p>
    <h2>Data servers</h2>
    <table id="DataServerTable" runat="server">
        <tr>
            <th scope="col">Name</th>
            <th scope="col">Rack</th>
            <th scope="col">Port</th>
            <th scope="col">Last contact</th>
            <th scope="col">Blocks</th>
            <th scope="col">Disk space</th>
            <th scope="col">Log file</th>
        </tr>
    </table>
</asp:Content>

