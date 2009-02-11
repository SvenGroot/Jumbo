<%@ Page Title="File system namespace - Jumbo DFS" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="filesystem.aspx.cs" Inherits="filesystem" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2>File system namespace</h2>
    <ul id="FileSystem">
        <li id="FileSystemRoot"><span><img onclick="loadDirectory(this);" class="directory" src="images/folder_open.png" alt="/" />/</span></li>
    </ul>
</asp:Content>

