$configDocGen = "..\..\ConfigurationDocumentationGenerator\ConfigurationDocumentationGenerator\bin\Release\ConfigurationDocumentationGenerator.exe"
$wordMLConversion = "..\..\WordMLConversion\WordMLConversion\bin\Release\WordMLConversion.exe"

"Updating XSD files"
&$configDocGen Reflect "..\Ookii.Jumbo.Jet\bin\Release\Ookii.Jumbo.dll" Ookii.Jumbo.JumboConfiguration ookii.jumbo "..\CommonConfiguration.xsd"
&$configDocGen Reflect "..\Ookii.Jumbo.Jet\bin\Release\Ookii.Jumbo.dll" Ookii.Jumbo.JumboConfiguration ookii.jumbo "CommonConfiguration.xsd"
&$configDocGen Reflect "..\Ookii.Jumbo.Jet\bin\Release\Ookii.Jumbo.Dfs.dll" Ookii.Jumbo.Dfs.DfsConfiguration ookii.jumbo.dfs "..\DfsConfiguration.xsd"
&$configDocGen Reflect "..\Ookii.Jumbo.Jet\bin\Release\Ookii.Jumbo.Dfs.dll" Ookii.Jumbo.Dfs.DfsConfiguration ookii.jumbo.dfs "DfsConfiguration.xsd"
&$configDocGen Reflect "..\Ookii.Jumbo.Jet\bin\Release\Ookii.Jumbo.Jet.dll" Ookii.Jumbo.Jet.JetConfiguration ookii.jumbo.jet "..\JetConfiguration.xsd"
&$configDocGen Reflect "..\Ookii.Jumbo.Jet\bin\Release\Ookii.Jumbo.Jet.dll" Ookii.Jumbo.Jet.JetConfiguration ookii.jumbo.jet "JetConfiguration.xsd"

"Updating XSD documentation"
&$configDocGen Document CommonConfiguration.xsd
&$configDocGen Document DfsConfiguration.xsd
&$configDocGen Document JetConfiguration.xsd
&$configDocGen Document JobConfiguration.xsd

"Updating WordML documentation"
&$wordMLConversion "Quick Start Guide.docx" -StyleSheet UserGuide -FormatCode
&$wordMLConversion "User Guide.docx" -StyleSheet UserGuide -FormatCode