<%@ Page Title="File system namespace - Jumbo DFS" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="filesystem.aspx.cs" Inherits="filesystem" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <div id="SideBar">
        <div id="SideBarInner">
            <div>
                <strong>Name: </strong><span id="EntryName"></span>
            </div>
            <div>
                <strong>Created: </strong><span id="EntryCreated"></span>
            </div>
            <div><strong>Full path:</strong></div>
            <div>
                <input id="FullPath" type="text" value=""/>
            </div>
            <div id="FileInfo" style="display:none">
                <div>
                    <strong>Size: </strong><span id="FileFormattedSize"></span> (<span id="FileSize"></span> bytes)
                </div>
                <div>
                    <strong>Block size: </strong><span id="BlockSize"></span>
                </div>
                <div>
                    <strong>Blocks: </strong><span id="BlockCount"></span>
                </div>
                <div>
                    <strong>Replicas: </strong><span id="ReplicationFactor"></span>
                </div>
                <div>
                    <strong>Record options: </strong><span id="RecordOptions"></span>
                </div>
                <div>
                    <a id="ViewFileLink" href="viewfile.aspx">View as text</a>
                </div>
                <div>
                    <a id="DownloadLink" href="downloadfile.ashx">Download</a>
                </div>
            </div>
        </div>
    </div>
    <div id="PageContent">
        <h2>File system namespace</h2>
        <ul id="FileSystem">
            <li id="FileSystemRoot"><span><img class="directory" src="images/arrow_closed.png" alt="expand" /><span><img src="images/folder_open.png" alt="/" />/</span></span></li>
        </ul>
    </div>
</asp:Content>

