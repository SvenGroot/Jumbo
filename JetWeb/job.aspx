<%@ Page Title="" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="job.aspx.cs" Inherits="job" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2 id="HeaderText" runat="server">Job</h2>
    <div id="JobSummary" runat="server">
        <table id="RunningJobsTable" runat="server">
            <tr>
                <th scope="col">Start time</th>
                <th scope="col">End time</th>
                <th scope="col">Duration</th>
                <th scope="col">Progress</th>
                <th scope="col">Tasks</th>
                <th scope="col">Running tasks</th>
                <th scope="col">Pending tasks</th>
                <th scope="col">Finished tasks</th>
                <th scope="col">Errors</th>
                <th scope="col">Non data local tasks</th>
            </tr>
        </table>
        <p>
            <a id="_downloadLink" href="jobinfo.ashx" visible="false" runat="server">Download job information.</a>
        </p>
        <h3>Stages</h3>
        <table id="StagesTable" runat="server">
            <tr>
                <th scope="col">Stage ID</th>
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
        <p>
            <a id="_allTasksLink" href="alltasks.aspx" runat="server">View details for all tasks.</a>
        </p>
        <h3>Metrics</h3>
        <table id="MetricsTable" runat="server">
            <tr>
                <td>&nbsp;</td>
            </tr>
            <tr>
                <th scope="row">Input records</th>
            </tr>
            <tr>
                <th scope="row">Input bytes</th>
            </tr>
            <tr>
                <th scope="row">Output records</th>
            </tr>
            <tr>
                <th scope="row">Output bytes</th>
            </tr>
            <tr>
                <th scope="row">DFS bytes read</th>
            </tr>
            <tr>
                <th scope="row">DFS bytes written</th>
            </tr>
            <tr>
                <th scope="row">Local bytes read</th>
            </tr>
            <tr>
                <th scope="row">Local bytes written</th>
            </tr>
            <tr>
                <th scope="row">Channel network bytes read</th>
            </tr>
            <tr>
                <th scope="row">Channel network bytes written</th>
            </tr>
        </table>
    </div>
</asp:Content>

