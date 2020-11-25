namespace CSharpDemo.IDEAs
{
    using AzureLib.KeyVault;
    using CSharpDemo.Azure.CosmosDB;
    using CSharpDemo.FileOperation;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    class DataBuildJsonFileOperation
    {
        private static string FolderPath = @"D:\IDEAs\pls_repos";


        public static void MainMethod()
        {
            QueryAllViewFiles();
        }
        public static void QueryAllViewFiles()
        {
            var jsonFilePaths = ReadFile.GetAllFilePath(FolderPath);
            HashSet<string> set = new HashSet<string>();
            foreach (var jsonFilePath in jsonFilePaths)
            {
                if (jsonFilePath.EndsWith(".view"))
                {
                    var lines = ReadFile.FifthMethod(jsonFilePath);
                    foreach (var line in lines)
                    {
                        if (line.Contains("\"/share"))
                        {
                            var startIndex = line.IndexOf("\"/share") + 1;
                            var endIndex = line.IndexOf("\"", startIndex);
                            if (startIndex < 1 || endIndex < 0)
                            {
                                continue;
                            }
                            var path = line.Substring(startIndex, endIndex - startIndex);
                            if (path.Contains(".ss") && !path.EndsWith(".ss"))
                            {
                                path = path.Substring(0, path.IndexOf(".ss") + 3);
                            }
                            else if (path.Contains(".view") && !path.EndsWith(".view"))
                            {
                                path = path.Substring(0, path.IndexOf(".view") + 5);

                            }
                            else if (path.Contains(".dll") && !path.EndsWith(".dll"))
                            {
                                path = path.Substring(0, path.IndexOf(".dll") + 4);

                            }
                            else if (path.Contains(".log") && !path.EndsWith(".log"))
                            {
                                path = path.Substring(0, path.IndexOf(".log") + 4);

                            }
                            else if (path.Contains(".tsv") && !path.EndsWith(".tsv"))
                            {
                                path = path.Substring(0, path.IndexOf(".tsv") + 4);

                            }
                            else if (path.Contains(".csv") && !path.EndsWith(".csv"))
                            {
                                path = path.Substring(0, path.IndexOf(".csv") + 4);

                            }
                            else if (path.Contains(".parquet") && !path.EndsWith(".parquet"))
                            {
                                path = path.Substring(0, path.IndexOf(".parquet") + 8);

                            }
                            set.Add(path);
                        }
                    }
                }
            }

            Console.WriteLine(set.Count);
            foreach (var path in set)
            {
                Console.WriteLine(path);
            }

            var list = set.ToList();
            list.Sort();
            WriteFile.Save(@"D:\data\company_work\M365\IDEAs\buildpath.tsv", list);
        }
    }
}
