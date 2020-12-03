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
        private static string GitRootPath = @"https://o365exchange.visualstudio.com/O365%20Core/_git/";

        public static void MainMethod()
        {
            //QueryAllViewFiles();
            QueryWrongStreamViewPaths();
        }

        private static void QueryAllViewFiles()
        {
            var filePaths = ReadFile.GetAllFilePath(FolderPath);
            HashSet<string> set = new HashSet<string>();
            foreach (var filePath in filePaths)
            {
                if (filePath.EndsWith(".view"))
                {
                    var lines = ReadFile.FifthMethod(filePath);
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
            WriteFile.Save(@"D:\data\company_work\M365\IDEAs\build\sharepaths.tsv", list);
        }

        private static void QueryWrongStreamViewPaths()
        {
            var ignoreStart = @"/shares/IDEAs.Prod.Data/";
            string noShareSettingTsvFilePath = @"D:\data\company_work\M365\IDEAs\build\nosharesettingpaths.tsv";
            string noPermissionTsvFilePath = @"D:\data\company_work\M365\IDEAs\build\nopermissionpaths.tsv";
            string shareTsvFilePath = @"D:\data\company_work\M365\IDEAs\build\sharepaths.tsv";
            var noShareSettingStreamPaths = ReadFile.FifthMethod(noShareSettingTsvFilePath);
            var noPermissionStreamPaths = ReadFile.FifthMethod(noPermissionTsvFilePath);
            var allStreamPaths = new HashSet<string>(ReadFile.FifthMethod(shareTsvFilePath));
            var noShareDict = new Dictionary<string, IList<string>>();
            var noPermissionDict = new Dictionary<string, IList<string>>();
            Console.WriteLine("noShareSettingStreamPaths: ");
            foreach (var noShareSettingStreamPath in noShareSettingStreamPaths)
            {
                if (noShareSettingStreamPath.StartsWith(ignoreStart))
                {
                    continue;
                }

                var shareName = GetPathPrefix(noShareSettingStreamPath).Substring(8);
                if (!noShareDict.ContainsKey(shareName))
                {
                    noShareDict.Add(shareName, new List<string>());
                }

                foreach (var streamPath in allStreamPaths)
                {
                    if (streamPath.StartsWith(noShareSettingStreamPath))
                    {
                        noShareDict[shareName].Add(noShareSettingStreamPath);
                    }
                }
            }

            Console.WriteLine("noPermissionStreamPaths: ");
            foreach (var noPermissionStreamPath in noPermissionStreamPaths)
            {
                if (noPermissionStreamPath.StartsWith(ignoreStart))
                {
                    continue;
                }

                var shareName = GetPathPrefix(noPermissionStreamPath).Substring(8);
                if (!noPermissionDict.ContainsKey(shareName))
                {
                    noPermissionDict.Add(shareName, new List<string>());
                }
                foreach (var streamPath in allStreamPaths)
                {
                    if (streamPath.StartsWith(noPermissionStreamPath))
                    {
                        noPermissionDict[shareName].Add(noPermissionStreamPath);
                    }
                }
            }

            var noShareLines = new List<string>();
            var noPermissionLines = new List<string>();
            var filePaths = ReadFile.GetAllFilePath(FolderPath);
            HashSet<string> set = new HashSet<string>();
            foreach (var filePath in filePaths)
            {
                if (filePath.EndsWith(".view"))
                {
                    var fileContent = ReadFile.ThirdMethod(filePath);
                    Console.WriteLine(filePath);
                    var splits = filePath.Split(new char[] { '\\' });
                    string repoName = splits[3];
                    string gitLink = GitRootPath + repoName + @"?path=";
                    for (int i = 4; i < splits.Length; i++)
                    {
                        gitLink += @"%2F" + splits[i];
                    }
                    foreach (var noShare in noShareDict)
                    {
                        foreach (var streamPath in noShare.Value)
                        {
                            if (fileContent.Contains(streamPath))
                            {
                                noShareLines.Add(noShare.Key + "\t" + streamPath + "\t" + repoName + "\t" + gitLink);
                            }
                        }
                    }

                    foreach (var noPermission in noPermissionDict)
                    {
                        foreach (var streamPath in noPermission.Value)
                        {
                            if (fileContent.Contains(streamPath))
                            {
                                noPermissionLines.Add(noPermission.Key + "\t" + streamPath + "\t" + repoName + "\t" + gitLink);
                            }
                        }
                    }
                }
            }
            WriteFile.Save(@"D:\data\company_work\M365\IDEAs\build\noShareLines.tsv", noShareLines);
            WriteFile.Save(@"D:\data\company_work\M365\IDEAs\build\noPermissionLines.tsv", noPermissionLines);
        }

        private static string GetPathPrefix(string streamPath)
        {
            streamPath = streamPath.Trim('/');
            if (string.IsNullOrEmpty(streamPath))
            {
                return string.Empty;
            }

            string pathPrefix = string.Empty;
            var splits = streamPath.Split(new char[] { '/' });

            if (streamPath.StartsWith("share"))
            {
                for (int i = 0; i < 3 && i < splits.Length; i++)
                {
                    pathPrefix += "/" + splits[i];

                }
            }
            else
            {
                for (int i = 0; i < 2 && i < splits.Length; i++)
                {
                    pathPrefix += "/" + splits[i];
                }
            }

            return pathPrefix;
        }
    }
}
