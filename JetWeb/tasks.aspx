<%@ Page Title="" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="tasks.aspx.cs" Inherits="tasks" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2 id="HeaderText" runat="server">Tasks</h2>
    <div id="StageSummary" runat="server">
        <table id="StagesTable" runat="server">
            <tr>
                <th scope="col">Stage name</th>
                <th scope="col">Start time</th>
                <th scope="col">End time</th>
                <th scope="col">Duration</th>
                <th scope="col">Progress</th>
                <th scope="col">Tasks</th>
                <th scope="col">Running tasks</th>
                <th scope="col">Pending tasks</th>
                <th scope="col">Finished tasks</th>
            </tr>
        </table>
        <h3 id="TasksHeader" runat="server">
            Tasks
        </h3>
        <table id="TasksTable" runat="server">
            <tr>
                <th scope="col">Task ID</th>
                <th scope="col">State</th>
                <th scope="col">Task Server</th>
                <th scope="col">Attempts</th>
                <th scope="col">Start time</th>
                <th scope="col">End time</th>
                <th scope="col">Duration</th>
                <th scope="col">Progress</th>
                <th scope="col">Log file</th>
            </tr>
        </table>
    </div>
</asp:Content>

