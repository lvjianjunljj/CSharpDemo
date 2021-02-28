namespace CSharpDemo.IDEAs
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;

    class CosmosViewErrorMessageOperation
    {
        private const string GitRootLink = @"https://o365exchange.visualstudio.com/O365%20Core/_git/";
        private const string PLSReposRootPath = @"D:\IDEAs\pls_repos";
        private static Regex NameNotExistMatchRegex = new Regex(
                @"E_CSC_USER_INVALIDCSHARP_0103: C# error CS0103: The name '(?<name>.*)' does not exist in the current context");
        private static Regex NamespaceNotExistMatchRegex = new Regex(
            @"E_CSC_USER_INVALIDCSHARP_0234: C# error CS0234: The type or namespace name '(?<namespace_1>.*)' does not exist in the namespace '(?<namespace_2>.*)'");
        private static Regex CloumnTypeMatchRegex = new Regex(
            @"column '(?<type_1>.*)' of type '(?<type_2>.*)'");
        private static Regex ExpectedTokenMatchRegex = new Regex(
                        @"Correct the script syntax, using expected token\(s\) as a guide\.\.\.\. at token '(?<token>.*)'");

        public static void MainMethod()
        {
            // Need to run "AzureCosmosDBOperation.GetTestRunMessagesForEveryDataset()" to upload all cosmos view file to git.
            CollectErrorMessage();
        }

        private static void CollectErrorMessage()
        {
            Console.WriteLine(@"Start CollectErrorMessage...");


            var allViewFilePaths = Directory.GetFiles(PLSReposRootPath, "*.view", SearchOption.AllDirectories);
            HashSet<string> viewFolders = new HashSet<string>();
            foreach (var viewFilePath in allViewFilePaths)
            {
                viewFolders.Add(Path.GetDirectoryName(viewFilePath));
            }


            Console.WriteLine(allViewFilePaths.Length);
            Console.WriteLine(viewFolders.Count);


            int count = 0;
            HashSet<ViewFileInfo> viewFileInfos = new HashSet<ViewFileInfo>(new ViewFileInfoeComparer());
            foreach (var viewFolder in viewFolders)
            {
                var infos = GetViewNames(viewFolder);
                foreach (var info in infos)
                {
                    count++;
                    viewFileInfos.Add(info);
                }
            }
            Console.WriteLine(viewFileInfos.Count);

            var testRunMessagesPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\allTestRuns.json";
            JArray testRunMessages = JArray.Parse(File.ReadAllText(testRunMessagesPath));


            int missCount = 0;
            foreach (var testRunMessage in testRunMessages)
            {
                string datasetId = testRunMessage["datasetId"].ToString();
                var messages = testRunMessage["messages"].ToArray();

                var datasetName = testRunMessage["datasetName"].ToString();
                var splits = datasetName.Split('/');
                var viewName = splits[0];
                var version = splits.Length > 1 ? splits[1] : string.Empty;

                var viewFileInfo = GetViewFileInfo(viewFileInfos, viewName, version);
                if (viewFileInfo == null)
                {
                    continue;
                }
                var viewGitLink = GenerateGitInfo(viewFileInfo.ViewFilePath, out string viewOwnerName, out string viewOwnerEmail);
                var jsonGitLink = GenerateGitInfo(viewFileInfo.JsonFilePath, out string jsonOwnerName, out string jsonOwnerEmail);
                Console.WriteLine($"{viewFileInfo.ViewName}\t{viewOwnerName}\t{viewOwnerEmail}");
                Console.WriteLine(viewGitLink);
                Console.WriteLine(jsonGitLink);

                Test(messages);
            }
            Console.WriteLine($"missCount: {missCount}");
            Console.WriteLine(@"End CollectErrorMessage...");
        }

        private static ViewFileInfo GetViewFileInfo(HashSet<ViewFileInfo> viewFileInfos, string viewName, string version)
        {
            ViewFileInfo viewFileInfo = null;
            var selectedViewFileInfos = viewFileInfos.Where(v => v.ViewName.Equals(viewName)).ToArray();

            if (selectedViewFileInfos.Length == 0)
            {
                //throw new FileNotFoundException($"There is no selected view file info for view name '{viewName}'");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"There is no selected view file info for view name '{viewName}'");
                Console.ForegroundColor = ConsoleColor.Gray;
            }
            else if (selectedViewFileInfos.Length == 1)
            {
                viewFileInfo = selectedViewFileInfos[0];
            }
            else
            {
                viewFileInfo = selectedViewFileInfos.Where(v => v.Version.Equals(version)).Single();
            }

            return viewFileInfo;
        }

        private static IEnumerable<ViewFileInfo> GetViewNames(string viewFileFolderPath)
        {
            var jsonFilePaths = Directory.GetFiles(viewFileFolderPath, "*.json", SearchOption.AllDirectories);
            foreach (var jsonFilePath in jsonFilePaths)
            {
                JObject json = JObject.Parse(File.ReadAllText(jsonFilePath));
                if (!json.ContainsKey("fileName") ||
                    !json["fileName"].ToString().EndsWith(@".view") ||
                    !json.ContainsKey("version") ||
                    !json.ContainsKey("name"))
                {
                    continue;
                }

                var viewFilePath = Path.Combine(Path.GetDirectoryName(jsonFilePath), json["fileName"].ToString());
                if (!File.Exists(viewFilePath))
                {
                    continue;
                }

                yield return new ViewFileInfo
                {
                    ViewName = json["name"].ToString(),
                    // We add a leading character "v" .
                    Version = @"v" + json["version"].ToString(),
                    JsonFilePath = jsonFilePath,
                    ViewFilePath = viewFilePath,
                };
            }
        }

        private static string GenerateGitInfo(string viewFilePath, out string ownerName, out string ownerEmail)
        {
            var splits = viewFilePath.Split(new char[] { '\\' });
            string repoName = splits[3];
            string gitLink = GitRootLink + repoName + @"?path=";

            for (int i = 4; i < splits.Length; i++)
            {
                gitLink += @"%2F" + splits[i];
            }

            ownerName = GetGitFileOwner(Path.Combine(PLSReposRootPath, repoName), viewFilePath, out ownerEmail);

            return gitLink;
        }

        // Get the owner of the git file based on the commits of the git file
        private static string GetGitFileOwner(string gitRepoFolderPath, string filePath, out string email)
        {
            var gitFileRelativePath = filePath.Substring(gitRepoFolderPath.Length + 1).Replace(@"\", @"/");
            var commitAuthorPairs = CommandRunDemo.GetGitCommitAuthors(gitRepoFolderPath, gitFileRelativePath);
            Dictionary<string, string> ownerEmails = new Dictionary<string, string>();
            Dictionary<string, int> ownerCounts = new Dictionary<string, int>();
            foreach (var commitAuthorPair in commitAuthorPairs)
            {
                var curName = commitAuthorPair.Key;
                var curEmail = commitAuthorPair.Value;
                if (!ownerEmails.ContainsKey(curName))
                {
                    ownerEmails.Add(curName, curEmail);
                }

                if (!ownerCounts.ContainsKey(curName))
                {
                    ownerCounts.Add(curName, 0);
                }

                ownerCounts[curName]++;
            }

            int maxCount = 0;
            string ownerName = string.Empty;
            foreach (var ownerCount in ownerCounts)
            {
                if (maxCount < ownerCount.Value)
                {
                    maxCount = ownerCount.Value;
                    ownerName = ownerCount.Key;
                }
            }

            email = ownerEmails[ownerName];
            return ownerName;
        }

        private static void Test(JToken[] messages)
        {
            string name = string.Empty;
            foreach (var message in messages)
            {
                //Console.WriteLine(message);
                var nameNotExistMatch = NameNotExistMatchRegex.Match(message.ToString());
                if (nameNotExistMatch.Success)
                {
                    var curName = nameNotExistMatch.Result("${name}").ToString();
                    if (string.IsNullOrEmpty(name) || !name.Equals(curName))
                    {
                        name = curName;
                        Console.WriteLine($"name: {name}");
                    }
                }
            }

            if (!string.IsNullOrEmpty(name))
            {
                return;
            }

            string namespace_1 = string.Empty;
            string namespace_2 = string.Empty;
            foreach (var message in messages)
            {
                var namespaceNotExistMatch = NamespaceNotExistMatchRegex.Match(message.ToString());
                if (namespaceNotExistMatch.Success)
                {
                    var cur_namespace_1 = namespaceNotExistMatch.Result("${namespace_1}").ToString();
                    var cur_namespace_2 = namespaceNotExistMatch.Result("${namespace_2}").ToString();
                    if (string.IsNullOrEmpty(namespace_1) ||
                        string.IsNullOrEmpty(namespace_2) ||
                        !namespace_1.Equals(cur_namespace_1) ||
                        !namespace_2.Equals(cur_namespace_2))
                    {
                        namespace_1 = cur_namespace_1;
                        namespace_2 = cur_namespace_2;
                        Console.WriteLine($"namespace_1: {namespace_1}");
                    }
                }
            }

            if (!string.IsNullOrEmpty(namespace_1) || !string.IsNullOrEmpty(namespace_2))
            {
                return;
            }

            string cloumnType_1 = string.Empty;
            string cloumnType_2 = string.Empty;
            foreach (var message in messages)
            {
                //Console.WriteLine(message);
                var rowsetCloumnTypeMatch = CloumnTypeMatchRegex.Match(message.ToString());
                if (rowsetCloumnTypeMatch.Success)
                {
                    var cur_cloumnType_1 = rowsetCloumnTypeMatch.Result("${type_1}").ToString();
                    var cur_cloumnType_2 = rowsetCloumnTypeMatch.Result("${type_2}").ToString();
                    if (string.IsNullOrEmpty(cloumnType_1) ||
                        string.IsNullOrEmpty(cloumnType_2) ||
                        !cloumnType_1.Equals(cur_cloumnType_1) ||
                        !cloumnType_2.Equals(cur_cloumnType_2))
                    {
                        cloumnType_1 = cur_cloumnType_1;
                        cloumnType_2 = cur_cloumnType_2;
                        Console.WriteLine($"cloumnType_1: {cloumnType_1}");
                    }
                }
            }

            if (!string.IsNullOrEmpty(cloumnType_1) || !string.IsNullOrEmpty(cloumnType_2))
            {
                return;
            }

            string token = string.Empty;
            foreach (var message in messages)
            {
                //Console.WriteLine(message);
                var expectedTokenMatch = ExpectedTokenMatchRegex.Match(message.ToString());
                if (expectedTokenMatch.Success)
                {
                    var curToken = expectedTokenMatch.Result("${token}").ToString();
                    if (string.IsNullOrEmpty(token) || !token.Equals(curToken))
                    {
                        token = curToken;
                        Console.WriteLine($"token: {token}");
                    }
                }
            }

            if (!string.IsNullOrEmpty(token))
            {
                return;
            }

            //Console.ForegroundColor = ConsoleColor.Red;
            //Console.WriteLine(testRunMessage["datasetId"]);
            // The default foreground color for console is gray.
            //Console.ForegroundColor = ConsoleColor.Gray;
        }
    }

    [Serializable]
    public class ViewFileInfo
    {
        public string ViewName { get; set; }
        public string Version { get; set; }
        public string JsonFilePath { get; set; }
        public string ViewFilePath { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        public override bool Equals(object obj)
        {

            return this.ToString().Equals(obj.ToString());
        }
    }

    public class ViewFileInfoeComparer : IEqualityComparer<ViewFileInfo>
    {
        public bool Equals(ViewFileInfo x, ViewFileInfo y)
        {
            return x.ToString().Equals(y.ToString());
        }

        public int GetHashCode(ViewFileInfo obj)
        {
            return obj.ToString().GetHashCode();
        }
    }
}
