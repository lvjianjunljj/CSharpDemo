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
            @"column '(?<column>.*)' of type '(?<type>.*)'");
        private static Regex ExpectedTokenMatchRegex = new Regex(
                        @"Correct the script syntax, using expected token\(s\) as a guide\.\.\.\. at token '(?<token>.*)'");
        private static Regex NoStructuredStreamTokenMatchRegex = new Regex(
                        @"Verify that streamset parameters are correct and that it contains at least one structured stream\.\.\.\. at token '(?<stream>.*)'");
        private static Regex NoViewStreamInfoMatchRegex = new Regex(
                        @"Could not get stream info for (?<view>.*\.view)");

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

            JArray detailsJArray = new JArray();
            var nameNotExistLines = new List<string>() { @"Wrong column name(The name 'XXXX' does not exist in the current context):", "View name\tName\tView file link\tJson file link\tOwner" };
            var namespaceNotExistLines = new List<string>() { @"Leak of reference(The type or namespace name 'XXXX' does not exist in the namespace 'YYYY'): ", "View name\tTarget namespace\tSource namespace\tView file link\tJson file link\tOwner" };
            var cloumnTypeLines = new List<string>() { @"Wrong column type(column 'XXXX' of type 'YYYY'):", "View name\tColumn\tType\tView file link\tJson file link\tOwner" };
            var expectedTokenLines = new List<string>() { @"Wrong token(Correct the script syntax, using expected token(s) as a guide.... at token 'XXXX'): ", "View name\tToken\tView file link\tJson file link\tOwner" };
            var structuredStreamTokenLines = new List<string>() { @"Stream set issue(Verify that streamset parameters are correct and that it contains at least one structured stream.... at token 'XXXX'): ", "View name\tStructured Stream\tView file link\tJson file link\tOwner" };
            var viewStreamInfoLines = new List<string>() { @"No view stream(Could not get stream info for XXXX): ", "View name\tStream\tView file link\tJson file link\tOwner" };
            var missedMessageLines = new List<string>() { @"Missed messages: ", "View name\tMissed message\tView file link\tJson file link\tOwner" };
            foreach (var testRunMessage in testRunMessages)
            {
                JObject detailJson = new JObject();
                //string datasetId = testRunMessage["datasetId"].ToString();
                var messages = testRunMessage["messages"].ToArray();

                var datasetName = testRunMessage["datasetName"].ToString();
                var splits = datasetName.Split('/');
                var viewName = splits[0];
                var version = splits.Length > 1 ? splits[1] : string.Empty;
                detailJson["viewName"] = viewName;

                var viewFileInfo = GetViewFileInfo(viewFileInfos, viewName, version);
                if (viewFileInfo == null)
                {
                    continue;
                }

                var viewGitLink = GenerateGitInfo(viewFileInfo.ViewFilePath, out string _, out string viewOwnerName, out string viewOwnerEmail);
                var jsonGitLink = GenerateGitInfo(viewFileInfo.JsonFilePath, out string _, out string jsonOwnerName, out string jsonOwnerEmail);
                //Console.WriteLine($"{viewFileInfo.ViewName}\t{viewOwnerName}\t{viewOwnerEmail}");

                var tag = ClassifyErrorMessages(messages, out string errorContent, out string errorMessage);
                detailJson["errorMessage"] = errorMessage;

                var lineMessage = $"{viewName}\t{errorContent}\t{viewGitLink}\t{jsonGitLink}\t{viewOwnerName}";
                switch (tag)
                {
                    case 1:
                        if (!errorContent.Equals("worldwide") && !errorContent.Equals("True") && !errorContent.Equals("False"))
                        {
                            nameNotExistLines.Add(lineMessage);
                        }
                        break;
                    case 2:
                        namespaceNotExistLines.Add(lineMessage);
                        break;
                    case 3:
                        cloumnTypeLines.Add(lineMessage);
                        break;
                    case 4:
                        expectedTokenLines.Add(lineMessage);
                        break;
                    case 5:
                        structuredStreamTokenLines.Add(lineMessage);
                        break;
                    case 6:
                        viewStreamInfoLines.Add(lineMessage);
                        break;
                    case 7:
                        missedMessageLines.Add(lineMessage);
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine(testRunMessage["datasetId"]);
                        //The default foreground color for console is gray.
                        Console.ForegroundColor = ConsoleColor.Gray;
                        break;
                    default:
                        Console.WriteLine($"Wrong tag value: {tag}");
                        break;
                }

                detailsJArray.Add(detailJson);
            }

            var lines = new List<string>();
            lines.AddRange(nameNotExistLines);
            lines.Add(string.Empty);
            lines.AddRange(namespaceNotExistLines);
            lines.Add(string.Empty);
            lines.AddRange(cloumnTypeLines);
            lines.Add(string.Empty);
            lines.AddRange(expectedTokenLines);
            lines.Add(string.Empty);
            lines.AddRange(structuredStreamTokenLines);
            lines.Add(string.Empty);
            lines.AddRange(viewStreamInfoLines);
            lines.Add(string.Empty);
            lines.AddRange(missedMessageLines);

            File.WriteAllLines(@"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\messageLines.tsv", lines);
            File.WriteAllText(@"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\details_attach.json", detailsJArray.ToString());


            Console.WriteLine($"missCount: {missedMessageLines.Count}");
            Console.WriteLine(@"End CollecterrorContent...");
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

        private static string GenerateGitInfo(string viewFilePath, out string repoName, out string ownerName, out string ownerEmail)
        {
            var splits = viewFilePath.Split(new char[] { '\\' });
            repoName = splits[3];
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

        // errorContent is the main content in the table, errorMessage is the exception string list from compile
        private static int ClassifyErrorMessages(JToken[] messages, out string errorContent, out string errorMessage)
        {
            errorMessage = string.Empty;
            string name = string.Empty;
            foreach (var message in messages)
            {
                var nameNotExistMatch = NameNotExistMatchRegex.Match(message.ToString());
                if (nameNotExistMatch.Success)
                {
                    var curName = nameNotExistMatch.Result("${name}").ToString();
                    var curValue = nameNotExistMatch.Value;
                    if (string.IsNullOrEmpty(name) || !name.Equals(curName))
                    {
                        errorMessage = message.ToString();
                        name = curName;
                    }
                }
            }

            if (!string.IsNullOrEmpty(name))
            {
                errorContent = name;
                return 1;
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
                        errorMessage = message.ToString();
                        namespace_1 = cur_namespace_1;
                        namespace_2 = cur_namespace_2;
                    }
                }
            }

            if (!string.IsNullOrEmpty(namespace_1) || !string.IsNullOrEmpty(namespace_2))
            {
                errorContent = $"{namespace_1}\t{namespace_2}";
                return 2;
            }

            string cloumn = string.Empty;
            string type = string.Empty;
            foreach (var message in messages)
            {
                var cloumnTypeMatch = CloumnTypeMatchRegex.Match(message.ToString());
                if (cloumnTypeMatch.Success)
                {
                    var cur_cloumn = cloumnTypeMatch.Result("${column}").ToString();
                    var cur_type = cloumnTypeMatch.Result("${type}").ToString();
                    if (string.IsNullOrEmpty(cloumn) ||
                        string.IsNullOrEmpty(type) ||
                        !cloumn.Equals(cur_cloumn) ||
                        !type.Equals(cur_type))
                    {
                        errorMessage = message.ToString();
                        cloumn = cur_cloumn;
                        type = cur_type;
                    }
                }
            }

            if (!string.IsNullOrEmpty(cloumn) || !string.IsNullOrEmpty(type))
            {
                errorContent = $"{cloumn}\t{type}";
                return 3;
            }

            string token = string.Empty;
            foreach (var message in messages)
            {
                var expectedTokenMatch = ExpectedTokenMatchRegex.Match(message.ToString());
                if (expectedTokenMatch.Success)
                {
                    var curToken = expectedTokenMatch.Result("${token}").ToString();
                    if (string.IsNullOrEmpty(token) || !token.Equals(curToken))
                    {
                        errorMessage = message.ToString();
                        token = curToken;
                    }
                }
            }

            if (!string.IsNullOrEmpty(token))
            {
                errorContent = token;
                return 4;
            }

            string stream = string.Empty;
            foreach (var message in messages)
            {
                var expectedTokenMatch = NoStructuredStreamTokenMatchRegex.Match(message.ToString());
                if (expectedTokenMatch.Success)
                {
                    var curStream = expectedTokenMatch.Result("${stream}").ToString();
                    if (string.IsNullOrEmpty(token) || !token.Equals(curStream))
                    {
                        errorMessage = message.ToString();
                        stream = curStream;
                    }
                }
            }

            if (!string.IsNullOrEmpty(stream))
            {
                errorContent = stream;
                return 5;
            }

            string view = string.Empty;
            foreach (var message in messages)
            {
                var noViewStreamInfoMatch = NoViewStreamInfoMatchRegex.Match(message.ToString());
                if (noViewStreamInfoMatch.Success)
                {
                    var curView = noViewStreamInfoMatch.Result("${view}").ToString();
                    if (string.IsNullOrEmpty(token) || !token.Equals(curView))
                    {
                        errorMessage = message.ToString();
                        view = curView;
                    }
                }
            }

            if (!string.IsNullOrEmpty(view))
            {
                errorContent = view;
                return 6;
            }

            errorContent = @"";
            return 7;
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
