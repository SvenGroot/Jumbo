<%@ Page Title="Remove data server - Jumbo DFS" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="removedataserver.aspx.cs" Inherits="removedataserver" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <form id="RemoveForm" runat="server">
    <h2>Remove data server</h2>
    <p>
        <asp:Button ID="_removeButton" OnClick="_removeButton_Click" Text="Remove" runat="server" />
        <a href="Default.aspx">Return to main page.</a>
    </p>
    </form>
</asp:Content>

