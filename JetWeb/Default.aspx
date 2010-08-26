<%@ Page Title="Jumbo Jet" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="Default.aspx.cs" Inherits="_Default" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2>Job server status</h2>
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
            <th scope="row">Non-input task capacity</th>
            <td id="NonInputTaskCapacityColumn" runat="server"></td>
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
        <a href="logfile.aspx">View job server log file.</a><br />
    </p>
    <h2>Task servers</h2>
    <table id="DataServerTable" runat="server">
        <tr>
            <th scope="col">Name</th>
            <th scope="col">Port</th>
            <th scope="col">Last contact</th>
            <th scope="col">Max tasks</th>
            <th scope="col">Max non-input tasks</th>
            <th scope="col">Log file</th>
        </tr>
    </table>
    <h2>Running jobs</h2>
    <table id="RunningJobsTable" runat="server">
        <tr>
            <th scope="col">Job ID</th>
            <th scope="col">Job Name</th>
            <th scope="col">Start time</th>
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
    <h2>Finished jobs</h2>
    <table id="FinishedJobsTable" runat="server">
        <tr>
            <th scope="col">Job ID</th>
            <th scope="col">Job Name</th>
            <th scope="col">Start time</th>
            <th scope="col">End time</th>
            <th scope="col">Duration</th>
            <th scope="col">Tasks</th>
            <th scope="col">Errors</th>
            <th scope="col">Non data local tasks</th>
        </tr>
    </table>
    <h2>Failed jobs</h2>
    <table id="FailedJobsTable" runat="server">
        <tr>
            <th scope="col">Job ID</th>
            <th scope="col">Job Name</th>
            <th scope="col">Start time</th>
            <th scope="col">End time</th>
            <th scope="col">Duration</th>
            <th scope="col">Tasks</th>
            <th scope="col">Errors</th>
            <th scope="col">Non data local tasks</th>
        </tr>
    </table>
</asp:Content>

