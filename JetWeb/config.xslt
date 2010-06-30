﻿<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:msxsl="urn:schemas-microsoft-com:xslt" exclude-result-prefixes="msxsl" xmlns="http://www.w3.org/1999/xhtml" xmlns:job="http://www.tkl.iis.u-tokyo.ac.jp/schema/Jumbo/JobConfiguration"
>
  <!-- $Id -->
  <xsl:output method="xml" indent="yes" doctype-public="-//W3C//DTD XHTML 1.1//EN" doctype-system="http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd"/>

  <xsl:template match="/">
    <html xml:lang="en-us">
      <head>
        <title>Job <xsl:value-of select="job:Job/@name"/> configuration</title>
        <style type="text/css">
          <![CDATA[
          body 
          {
	          font-family: 'Segoe UI', Verdana, sans-serif;
	          font-size: small;
	          background: white;
	          color: black;
          }
          table
          {
              border-collapse: collapse;
          }
          th, td
          {
              padding: 0.2em 0.5em;
          }
          th
          {
              white-space: nowrap;
              text-align: left;
              vertical-align: top;
          }
          th[scope="colgroup"]
          {
            text-align: center;
          }
          .stage
          {
            margin: 1em 0 0 0;
            border: solid 3px white;
            background-color: #ccc;
            padding: 0.3em;
          }
          .stage h3
          {
            margin: 0 0 0.5em 0;
          }
          .output
          {
            margin: 1em 0 0 0;
            border: solid 3px white;
            background-color: #cce;
          }
          .input
          {
            margin: 1em 0 0 0;
            border: solid 3px white;
            background-color: #cec;
          }
          h4
          {
            font-style: italic;
            margin: 1em 0 0 0;
          }
        ]]>
        </style>
        <script type="text/javascript">
          <xsl:text disable-output-escaping="yes">
          <![CDATA[
              window.onload = function () {
                  var spans = document.getElementsByTagName("span");
                  for( var x = 0; x < spans.length; ++x ) 
                  {
                      var span = spans.item(x);
                      if( span.className == "type" )
                      {
                          span.title = span.firstChild.nodeValue;
                          span.firstChild.nodeValue = parseType(span.firstChild.nodeValue);
                      }
                  }
              }

              function parseType(type) {
                  var genericArgumentCountIndex = type.indexOf("`");
                  if( genericArgumentCountIndex >= 0 ) 
                  {
                      var typeName = type.substr(0, genericArgumentCountIndex);
                      var argumentIndex = type.indexOf("[", genericArgumentCountIndex);
                      var lastArgumentIndex = type.lastIndexOf("]");
                      var arguments = type.substr(argumentIndex + 1, lastArgumentIndex - argumentIndex - 1);

                      type = type.substr(0, genericArgumentCountIndex) + "<" + parseTypeArguments(arguments) + ">";
                  }
                  else
                  {
                      var assemblyNameIndex = type.indexOf(",");
                      if( assemblyNameIndex >= 0 )
                          type = type.substr(0, assemblyNameIndex);
                  }
                  return type;
              }

              function parseTypeArguments(arguments)
              {
                  var bracketCount = 0;
                  var index = 0;
                  var result = "";
                  //alert(arguments);
                  while( index < arguments.length )
                  {
                      var start = index;
                      do
                      {
                          if( arguments.charAt(index) == '[' )
                              bracketCount++;
                          else if( arguments.charAt(index) == ']' )
                              bracketCount--;
                          index++;
                      } while( bracketCount > 0 && index < arguments.length );

                      if( start != 0 )
                          result += ", ";
                      result += parseType(arguments.substr(start + 1, index - start - 2));

                      index++; // skip the comma   
                  }
                  return result;
              }
          ]]>
          </xsl:text>
        </script>
      </head>
      <body>
        <h1>
          Job <xsl:value-of select="job:Job/@name"/> configuration
        </h1>
        <h2>Stages</h2>
        <xsl:apply-templates select="job:Job/job:Stages/job:Stage" />
        <xsl:apply-templates select="job:Job/job:JobSettings" />
        <xsl:apply-templates select="job:Job/job:AssemblyFileNames" />
      </body>
    </html>
  </xsl:template>
  <xsl:template match="job:Stage | job:ChildStage">
    <div class="stage">
      <xsl:attribute name="id">
        <xsl:apply-templates select="." mode="compoundStageId" />
      </xsl:attribute>
      <h3>
        <xsl:value-of select="@id"/>
      </h3>
      <table>
        <tr>
          <th scope="row">Task type:</th>
          <td>
            <span class="type">
              <xsl:value-of select="job:TaskType"/>
            </span>
          </td>
        </tr>
        <tr>
          <th scope="row">Task count:</th>
          <td>
            <xsl:value-of select="@taskCount"/>
          </td>
        </tr>
        <xsl:if test="job:MultiInputRecordReaderType!=''">
          <tr>
            <th scope="row">Multi input record reader:</th>
            <td>
              <span class="type">
                <xsl:value-of select="job:MultiInputRecordReaderType"/>
              </span>
            </td>
          </tr>
        </xsl:if>
        <xsl:if test="job:DependentStages!=''">
          <tr>
            <th scope="row">Dependent stages:</th>
            <td>
              <xsl:for-each select="job:DependentStages/job:string">
                <xsl:if test="position()!=1">
                  <xsl:text>, </xsl:text>
                </xsl:if>
                <a href="#{.}"><xsl:value-of select="."/></a>
              </xsl:for-each>
            </td>
          </tr>
        </xsl:if>
        <xsl:if test="job:ChildStage/@taskCount > 1">
          <tr>
            <th scrope="row">Child stage partitioner:</th>
            <td>
              <span class="type">
                <xsl:value-of select="job:ChildStagePartitionerType"/>
              </span>
            </td>
          </tr>
        </xsl:if>
      </table>
      <xsl:apply-templates select="." mode="input" />
      <xsl:apply-templates select="job:ChildStage" />
      <xsl:apply-templates select="job:OutputChannel"/>
      <xsl:apply-templates select="job:DfsOutput"/>
      <xsl:apply-templates select="job:StageSettings"/>
    </div>
  </xsl:template>
  <xsl:template match="job:OutputChannel">
    <table class="output">
      <tr>
        <th scope="row">
          <xsl:value-of select="@type"/> channel to stage:
        </th>
        <td>
          <a href="#{job:OutputStage}"><xsl:value-of select="job:OutputStage"/></a>
          <xsl:if test="@forceFileDownload='true'">
            <xsl:text> (force file download)</xsl:text>
          </xsl:if>
        </td>
      </tr>
      <xsl:if test="@partitionsPerTask>1">
        <tr>
          <th scope="row">Partitions per task</th>
          <td>
            <xsl:value-of select="@partitionsPerTask"/>
            <xsl:text> (</xsl:text>
            <xsl:value-of select="@partitionAssignmentMethod"/>
            <xsl:text> assignment)</xsl:text>
          </td>
        </tr>
      </xsl:if>
      <xsl:if test="/job:Job/job:Stages/job:Stage[@id=current()/job:OutputStage]/@taskCount>1">
        <tr>
          <th scope="row">Partitioner:</th>
          <td>
            <span class="type">
              <xsl:value-of select="job:PartitionerType"/>
            </span>
          </td>
        </tr>
      </xsl:if>
      <xsl:if test="../@taskCount>1">
        <tr>
          <th scope="row">Multi input record reader:</th>
          <td>
            <span class="type">
              <xsl:value-of select="job:MultiInputRecordReaderType"/>
            </span>
          </td>
        </tr>
      </xsl:if>
    </table>
  </xsl:template>
  <xsl:template match="job:DfsOutput">
    <table class="output">
      <tr>
        <th scope="row">DFS output:</th>
        <td>
          <xsl:value-of select="@path"/>
        </td>
      </tr>
      <tr>
        <th scope="row">Record writer:</th>
        <td>
          <span class="type">
            <xsl:value-of select="job:RecordWriterType"/>
          </span>
        </td>
      </tr>
      <xsl:if test="@blockSize>0">
        <tr>
          <th scope="row">Block size:</th>
          <td>
            <xsl:value-of select="format-number(@blockSize, '###,##0')"/>
          </td>
        </tr>
      </xsl:if>
      <xsl:if test="@replicationFactor>0">
        <tr>
          <th scope="row">Replication factor:</th>
          <td>
            <xsl:value-of select="@replicationFactor"/>
          </td>
        </tr>
      </xsl:if>
    </table>
  </xsl:template>
  <xsl:template match="job:JobSettings | job:StageSettings">
    <xsl:apply-templates select="." mode="title" />
    <table>
      <xsl:apply-templates select="job:Setting" />
    </table>
  </xsl:template>
  <xsl:template match="job:JobSettings" mode="title">
    <h2>Job settings</h2>
  </xsl:template>
  <xsl:template match="job:StageSettings" mode="title">
    <h4>Stage settings</h4>
  </xsl:template>
  <xsl:template match="job:Setting">
    <tr>
      <th scope="row">
        <xsl:value-of select="@key"/>
        <xsl:text>:</xsl:text>
      </th>
      <td>
        <xsl:value-of select="@value"/>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="job:Stage[job:DfsInput or //job:OutputChannel/job:OutputStage=@id or //job:DependentStages/job:string=@id]" mode="input">
    <table class="input">
      <xsl:apply-templates select="job:DfsInput" />
      <xsl:apply-templates select="//job:OutputChannel[job:OutputStage=current()/@id]" mode="input" />
      <xsl:if test="//job:DependentStages/job:string=current()/@id">
        <tr>
          <th scope="row">Scheduling dependencies:</th>
          <td>
            <xsl:apply-templates select="//job:DependentStages/job:string[.=current()/@id]" mode="input" />
          </td>
        </tr>
      </xsl:if>
    </table>
  </xsl:template>
  <xsl:template match="job:Stage | job:ChildStage" mode="input"></xsl:template>
  <xsl:template match="job:DfsInput">
    <tr>
      <th scope="row">DFS input:</th>
      <td>
        <xsl:value-of select="job:TaskInputs/job:TaskDfsInput/@path"/>
        <xsl:text>, block </xsl:text>
        <xsl:value-of select="job:TaskInputs/job:TaskDfsInput/@block"/>
        <xsl:if test="count(job:TaskInputs/job:TaskDfsInput)>1">
          <xsl:text> (and </xsl:text>
          <xsl:value-of select="count(job:TaskInputs/job:TaskDfsInput)-1"/>
          <xsl:text> others).</xsl:text>
        </xsl:if>
      </td>
    </tr>
    <tr>
      <th scope="row">Record reader:</th>
      <td>
        <span class="type">
          <xsl:value-of select="job:RecordReaderType"/>
        </span>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="job:OutputChannel" mode="input">
    <tr>
      <th scope="row">
        <xsl:value-of select="@type"/>
        <xsl:text> channel from stage:</xsl:text>
      </th>
      <td>
        <xsl:variable name="sendingStageId">
          <xsl:apply-templates select=".." mode="compoundStageId" />
        </xsl:variable>
        <a href="#{$sendingStageId}">
          <xsl:value-of select="$sendingStageId"/>
        </a>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="job:ChildStage" mode="compoundStageId">
    <xsl:apply-templates select=".." mode="compoundStageId" />
    <xsl:text>.</xsl:text>
    <xsl:value-of select="@id"/>
  </xsl:template>
  <xsl:template match="job:Stage" mode="compoundStageId">
    <xsl:value-of select="@id"/>
  </xsl:template>
  <xsl:template match="job:DependentStages/job:string" mode="input">
    <xsl:if test="position()>1">
      <xsl:text>, </xsl:text>
    </xsl:if>
    <xsl:variable name="dependencyStageId">
      <xsl:apply-templates select="../.." mode="compoundStageId" />
    </xsl:variable>
    <a href="#{$dependencyStageId}">
      <xsl:value-of select="$dependencyStageId"/>
    </a>
  </xsl:template>
  <xsl:template match="job:AssemblyFileNames">
    <h2>Assemblies</h2>
    <ul>
      <xsl:for-each select="job:string">
        <li>
          <xsl:value-of select="."/>
        </li>
      </xsl:for-each>
    </ul>
  </xsl:template>
</xsl:stylesheet>
