<%@ Page Title="Set safe mode - Jumbo DFS" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="setsafemode.aspx.cs" Inherits="setsafemode" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <form id="SafeModeForm" runat="server">
        <h2 id="_result" runat="server"></h2>
        <p>
            <asp:Button ID="_safeModeButton" OnClick="_safeModeButton_Click" Text="Confirm" runat="server" />
            <a href="Default.aspx">Return to main page.</a>
        </p>
    </form>
</asp:Content>

