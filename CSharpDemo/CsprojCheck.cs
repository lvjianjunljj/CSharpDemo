namespace CSharpDemo
{
    using System;
    using System.IO;

    public class CsprojCheck
    {
        public static void MainMethod()
        {
            CheckReferenceHintPathDemo();
        }

        private static void CheckReferenceHintPathDemo()
        {
            Console.WriteLine("Start...");
            var hintPathStartContent = @"<HintPath>";
            var hintPathEndContent = @"</HintPath>";
            string csprojFolderPath = @"D:\IDEAs\repos\Ibiza\Source\Services\DataCop\Microsoft.IDEAs.DataCop.AzureDataLakeLib";
            string csprojFileName = @"Microsoft.IDEAs.DataCop.AzureDataLakeLib.csproj";

            string csprojFilePath = Path.Combine(csprojFolderPath, csprojFileName);
            var lines = File.ReadAllLines(csprojFilePath);
            foreach (var line in lines)
            {
                if (line.Contains(hintPathStartContent))
                {
                    int startIndex = line.IndexOf(hintPathStartContent);
                    int endIndex = line.IndexOf(hintPathEndContent);
                    var packageRelativePath = line.Substring(startIndex + hintPathStartContent.Length, endIndex - startIndex - hintPathStartContent.Length);
                    string packagePath = Path.Combine(csprojFolderPath, packageRelativePath);
                    if (!File.Exists(packagePath))
                    {
                        Console.WriteLine(line);
                    }
                }
            }

            Console.WriteLine("End...");
        }
    }
}
