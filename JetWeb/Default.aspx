<%@ Page Title="Jumbo Jet" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="Default.aspx.cs" Inherits="_Default" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2>File system status</h2>
    <table>
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
</asp:Content>

