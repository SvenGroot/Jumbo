<%@ Page Title="" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="archive.aspx.cs" Inherits="archive" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2>
        Archived jobs
    </h2>
    <p>
        NOTE: Viewing log files or downloading a job summary for an archived job only works if all the task servers that were used for the job are
        still part of the cluster and still have the job's log files stored.
    </p>
    <table id="ArchivedJobsTable" runat="server">
        <tr>
            <th scope="col">Job ID</th>
            <th scope="col">Job Name</th>
            <th scope="col">Status</th>
            <th scope="col">Start time</th>
            <th scope="col">End time</th>
            <th scope="col">Duration</th>
            <th scope="col">Tasks</th>
        </tr>
    </table>
</asp:Content>

