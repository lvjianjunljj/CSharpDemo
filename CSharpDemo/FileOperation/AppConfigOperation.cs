namespace CSharpDemo.FileOperation
{
    using System;
    using System.Collections.Generic;
    using System.Xml;

    public class AppConfigOperation
    {
        private static string FolderPath = @"D:\IDEAs\repos\Ibiza\Source\Service\DataCop";
        public static void MainMethod()
        {
            UpdateKeyValuePair(null, null);
        }

        private static void UpdateKeyValuePair(string filePath, Dictionary<string, string> pairs)
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
                Console.WriteLine($"{xmlReader.Name}: {xmlReader.}");
            }
        }
    }
}
