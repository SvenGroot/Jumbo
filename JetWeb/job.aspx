<%@ Page Title="" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="job.aspx.cs" Inherits="job" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2 id="HeaderText" runat="server">Job</h2>
    <div id="JobSummary" runat="server">
        <table id="RunningJobsTable" runat="server">
            <tr>
                <th scope="col" rowspan="2">Start time</th>
                <th scope="col" rowspan="2">End time</th>
                <th scope="col" rowspan="2">Duration</th>
                <th scope="col" rowspan="2">Progress</th>
                <th scope="colgroup" colspan="7">Tasks</th>
            </tr>
            <tr>
                <th scope="col">Total</th>
                <th scope="col">Running</th>
                <th scope="col">Pending</th>
                <th scope="col">Finished</th>
                <th scope="col">Errors</th>
                <th scope="col">Rack local</th>
                <th scope="col">Non data local</th>
            </tr>
        </table>
        <p>
            <a id="_configLink" href="jobconfig.ashx" runat="server">View job configuration</a>
        </p>
        <p>
            <a id="_downloadLink" href="jobinfo.ashx" visible="false" runat="server">Download job information.</a>
        </p>
        <h3>Stages</h3>
        <table id="StagesTable" runat="server">
            <tr>
                <th scope="col" rowspan="2">Stage name</th>
                <th scope="col" rowspan="2">Start time</th>
                <th scope="col" rowspan="2">End time</th>
                <th scope="col" rowspan="2">Duration</th>
                <th scope="col" rowspan="2">Progress</th>
                <th scope="colgroup" colspan="4">Tasks</th>
            </tr>
            <tr>
                <th scope="col">Total</th>
                <th scope="col">Running</th>
                <th scope="col">Pending</th>
                <th scope="col">Finished</th>            
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
            <tr>
                <th scope="row">Dynamically assigned partitions</th>
            </tr>
            <tr>
                <th scope="row">Discarded partitions</th>
            </tr>
        </table>
    </div>
</asp:Content>

