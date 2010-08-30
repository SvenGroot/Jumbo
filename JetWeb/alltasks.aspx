<%@ Page Title="" Language="C#" MasterPageFile="~/Jumbo.master" AutoEventWireup="true" CodeFile="alltasks.aspx.cs" Inherits="alltasks" %>
<asp:Content ID="Content1" ContentPlaceHolderID="MainContentPlaceHolder" Runat="Server">
    <h2 id="HeaderText" runat="server">Job</h2>
    <table id="RunningJobsTable" runat="server">
        <tr>
            <th scope="col">Job ID</th>
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
    <script type="text/javascript" src="Silverlight.js"></script>
    <script type="text/javascript">
        function onSilverlightError(sender, args) {

            var appSource = "";
            if (sender != null && sender != 0) {
                appSource = sender.getHost().Source;
            }
            var errorType = args.ErrorType;
            var iErrorCode = args.ErrorCode;

            var errMsg = "Unhandled Error in Silverlight 3 Application " + appSource + "\n";

            errMsg += "Code: " + iErrorCode + "    \n";
            errMsg += "Category: " + errorType + "       \n";
            errMsg += "Message: " + args.ErrorMessage + "     \n";

            if (errorType == "ParserError") {
                errMsg += "File: " + args.xamlFile + "     \n";
                errMsg += "Line: " + args.lineNumber + "     \n";
                errMsg += "Position: " + args.charPosition + "     \n";
            }
            else if (errorType == "RuntimeError") {
                if (args.lineNumber != 0) {
                    errMsg += "Line: " + args.lineNumber + "     \n";
                    errMsg += "Position: " + args.charPosition + "     \n";
                }
                errMsg += "MethodName: " + args.methodName + "     \n";
            }

            throw new Error(errMsg);
        }
    </script>
    <div id="silverlightControlHost">
		<object data="data:application/x-silverlight-2," type="application/x-silverlight-2" width="100%" height="100%">
			<param name="source" value="ClientBin/JumboExecutionVisualizer.xap"/>
			<param name="onerror" value="onSilverlightError" />
			<param name="background" value="white" />
			<param name="minRuntimeVersion" value="3.0.50106.0" />
			<param name="autoUpgrade" value="true" />
			<param name="initParams" value="prefix=ctl00_MainContentPlaceHolder_" />
			<a href="http://go.microsoft.com/fwlink/?LinkID=149156&amp;v=3.0.50106.0" style="text-decoration: none;">
     			<img src="http://go.microsoft.com/fwlink/?LinkId=108181" alt="Get Microsoft Silverlight" style="border-style: none"/>
			</a>
		</object>
		<iframe style='visibility:hidden;height:0;width:0;border:0px'></iframe>
    </div>      
    <h3>Tasks</h3>
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
    <asp:PlaceHolder ID="_failedTaskAttemptsPlaceHolder" Visible="false" runat="server">
        <h3>Failed task attempts</h3>
        <table id="_failedTaskAttemptsTable" runat="server">
            <tr>
                <th scope="col">Task ID</th>
                <th scope="col">State</th>
                <th scope="col">Task Server</th>
                <th scope="col">Attempt</th>
                <th scope="col">Start time</th>
                <th scope="col">End time</th>
                <th scope="col">Duration</th>
                <th scope="col">Log file</th>
            </tr>        
        </table>
    </asp:PlaceHolder>
</asp:Content>

