<%@ Page Title="Jumbo Jet" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="Default.aspx.cs" Inherits="_Default" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2>Job server status</h2>
    <p class="error" id="ErrorMessage" runat="server" visible="false"></p>
    <table>
        <tr>
            <th scope="row">Job server</th>
            <td id="JobServerColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Running jobs</th>
            <td id="RunningJobsColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Finished jobs</th>
            <td id="FinishedJobsColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Failed jobs</th>
            <td id="FailedJobsColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Capacity</th>
            <td id="CapacityColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Scheduler</th>
            <td id="SchedulerColumn" runat="server"></td>
        </tr>
        <tr>
            <th scope="row">Task servers</th>
            <td id="TaskServersColumn" runat="server"></td>
        </tr>
    </table>
    <p>
        View job server log file: <a href="logfile.aspx?maxSize=100KB">last 100KB</a>, <a href="logfile.aspx?maxSize=0">all</a><br />
        <a href="archive.aspx">View archived jobs</a>
    </p>
    <h2>Task servers</h2>
    <table id="DataServerTable" runat="server">
        <tr>
            <th scope="col">Name</th>
            <th scope="col">Port</th>
            <th scope="col">Rack</th>
            <th scope="col">Last contact</th>
            <th scope="col">Task slots</th>
            <th scope="col">Log file</th>
        </tr>
    </table>
    <h2>Running jobs</h2>
    <table id="RunningJobsTable" runat="server">
        <tr>
            <th scope="col" rowspan="2">Job ID</th>
            <th scope="col" rowspan="2">Job Name</th>
            <th scope="col" rowspan="2">Start time</th>
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
    <h2>Finished jobs</h2>
    <table id="FinishedJobsTable" runat="server">
        <tr>
            <th scope="col" rowspan="2">Job ID</th>
            <th scope="col" rowspan="2">Job Name</th>
            <th scope="col" rowspan="2">Start time</th>
            <th scope="col" rowspan="2">End time</th>
            <th scope="col" rowspan="2">Duration</th>
            <th scope="colgroup" colspan="4">Tasks</th>
        </tr>
        <tr>
            <th scope="col">Total</th>
            <th scope="col">Errors</th>
            <th scope="col">Rack local</th>
            <th scope="col">Non data local</th>
        </tr>
    </table>
    <h2>Failed jobs</h2>
    <table id="FailedJobsTable" runat="server">
        <tr>
            <th scope="col" rowspan="2">Job ID</th>
            <th scope="col" rowspan="2">Job Name</th>
            <th scope="col" rowspan="2">Start time</th>
            <th scope="col" rowspan="2">End time</th>
            <th scope="col" rowspan="2">Duration</th>
            <th scope="colgroup" colspan="4">Tasks</th>
        </tr>
        <tr>
            <th scope="col">Total</th>
            <th scope="col">Errors</th>
            <th scope="col">Rack local</th>
            <th scope="col">Non data local</th>
        </tr>
    </table>
</asp:Content>

