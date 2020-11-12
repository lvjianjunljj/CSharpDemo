namespace CSharpDemo.FileOperation
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Xml;

    public class AppConfigOperation
    {
        private static string FolderPath = @"D:\IDEAs\repos\Ibiza\Source\Services\DataCop";
        public static void MainMethod()
        {
            UpdateToDataCopTorus();
        }
        public static void UpdateToDataCopTorus()
        {
            var pairs = new Dictionary<string, string>
            {
                ["KeyVaultName"] = "datacop-ppe",
                ["Environment"] = "ppe",
                ["TenantId"] = "cdc5aeea-15c5-4db6-b079-fcadd2505dc2",
            };
            var configFilePaths = Directory.GetFiles(FolderPath, "*pp.config", SearchOption.AllDirectories);
            foreach (var configFilePath in configFilePaths)
            {
                Console.WriteLine(configFilePath);
                UpdateKeyValuePair(configFilePath, pairs);
            }
        }

        private static void UpdateKeyValuePair(string configFilePath, Dictionary<string, string> pairs)
        {
            var content = File.ReadAllText(configFilePath, Encoding.UTF8);
            foreach (var key in pairs.Keys)
            {
                var queryContent = $"<add key=\"{key}\" value=\"";
                var index = content.IndexOf(queryContent);
                while (index >= 0)
                {
                    var start = index + queryContent.Length;
                    var end = content.IndexOf("\"", start);
                    content = content.Substring(0, start) + pairs[key] + content.Substring(end);
                    index = content.IndexOf(queryContent, end);
                }
            }

            WriteFile.FirstMethod(configFilePath, content);
        }

        // Not work as I expect
        private static void DemoFunction()
        {
            XmlTextReader xmlReader = new XmlTextReader(
                @"D:\IDEAs\repos\Ibiza\Source\Services\DataCop\Common\app.config");
            while (xmlReader.Read())
            {
                //switch (xmlReader.NodeType)
                //{
                //case XmlNodeType.Element:
                //    System.Console.WriteLine("<" + xmlReader.Name + ">");
                //    break;
                //case XmlNodeType.Text:
                //    System.Console.WriteLine(xmlReader.Value);
                //    break;
                //case XmlNodeType.EndElement:
                //    System.Console.WriteLine("");
                //    break;
                //}
                Console.WriteLine($"{xmlReader.Name}: {xmlReader.Value}");
            }
        }
    }
}
