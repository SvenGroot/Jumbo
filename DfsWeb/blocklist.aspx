<%@ Page Title="Block list - Jumbo DFS" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="blocklist.aspx.cs" Inherits="blocklist" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2 id="_pageTitle" runat="server">Block list</h2>
    <p>
        <a id="ShowFilesLink" href="blocklist.aspx" runat="server">Include files</a>
    </p>
    <ul id="_blockList" class="blockList" runat="server">
    </ul>
</asp:Content>

