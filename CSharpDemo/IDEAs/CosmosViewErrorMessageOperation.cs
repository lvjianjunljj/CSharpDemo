namespace CSharpDemo.IDEAs
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;

    public class CosmosViewErrorMessageOperation
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
        private static Regex UserDefinedErrorMatchRegex = new Regex(
                        "E_CSC_USER_PREPROCESSORUSERDEFINEDERROR: User defined error: \"(?<error>.*)\"\nDescription");
        private static Regex SameSourceMatchRegex = new Regex(
                        @"Script contains two different (resources|references) with identical file name: '(?<file>.*)'");
        private static Regex FileNotFoundMatchRegex = new Regex(
                        @"E_STORE_USER_FILENOTFOUND: File not found or access denied: '(?<file>.*)'");
        private static Regex DynamicViewMatchRegex = new Regex(
                        @"E_CSC_USER_DYNAMICVIEWADDITIONALRESOURCE: Dynamic View referenced an additional resource '(?<resource>.*)'");

        public static string RootFolderPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\udp_mdp";
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


            Console.WriteLine($"View Files Count {allViewFilePaths.Length}");
            Console.WriteLine($"View Folders Count: {viewFolders.Count}");


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
            Console.WriteLine($"View File Infos Count: {viewFileInfos.Count}");

            var testRunMessagesPath = Path.Combine(CosmosViewErrorMessageOperation.RootFolderPath, @"allTestRuns.json");
            JArray testRunMessages = JArray.Parse(File.ReadAllText(testRunMessagesPath));

            HashSet<string> dataFactoryNames = new HashSet<string>();
            foreach (var testRunMessage in testRunMessages)
            {
                string dataFactoryName = testRunMessage["dataFactoryName"].ToString();
                dataFactoryNames.Add(dataFactoryName);
            }

            foreach (var dataFactoryName in dataFactoryNames)
            {
                WriteSummaryResultToJsonFile(dataFactoryName, testRunMessages, viewFileInfos);
            }



            Console.WriteLine(@"End CollecterrorContent...");
        }

        private static void WriteSummaryResultToJsonFile(string dataFactoryName, JArray testRunMessages, HashSet<ViewFileInfo> viewFileInfos)
        {
            JArray detailsJArray = new JArray();
            var nameNotExistLines = new List<string>() { @"Wrong column name(The name 'XXXX' does not exist in the current context):", "View name\tName\tView file link\tJson file link\tOwner" };
            var namespaceNotExistLines = new List<string>() { @"Leak of reference(The type or namespace name 'XXXX' does not exist in the namespace 'YYYY'): ", "View name\tTarget namespace\tSource namespace\tView file link\tJson file link\tOwner" };
            var cloumnTypeLines = new List<string>() { @"Wrong column type(column 'XXXX' of type 'YYYY'):", "View name\tColumn\tType\tView file link\tJson file link\tOwner" };
            var expectedTokenLines = new List<string>() { @"Wrong token(Correct the script syntax, using expected token(s) as a guide.... at token 'XXXX'): ", "View name\tToken\tView file link\tJson file link\tOwner" };
            var structuredStreamTokenLines = new List<string>() { @"Stream set issue(Verify that streamset parameters are correct and that it contains at least one structured stream.... at token 'XXXX'): ", "View name\tStructured Stream\tView file link\tJson file link\tOwner" };
            var viewStreamInfoLines = new List<string>() { @"No view stream(Could not get stream info for XXXX): ", "View name\tStream\tView file link\tJson file link\tOwner" };
            var userDefinedErrorLines = new List<string>() { @"User defined error(User defined error: XXXX): ", "View name\tError\tView file link\tJson file link\tOwner" };
            var sameSourceLines = new List<string>() { @"Two different resources/references(Script contains two different resources/references with identical file name: 'XXXX)': ", "View name\tFileName\tView file link\tJson file link\tOwner" };
            var fileNotFoundLines = new List<string>() { @"File not found or access denied(E_STORE_USER_FILENOTFOUND: File not found or access denied: 'XXXX'): ", "View name\tFilePath\tView file link\tJson file link\tOwner" };
            var dynamicViewLines = new List<string>() { @"Aditional resource(Dynamic View referenced an additional resource 'XXXX'): ", "View name\tResource\tView file link\tJson file link\tOwner" };



            var missedMessageLines = new List<string>() { @"Missed messages: ", "View name\tMissed message\tView file link\tJson file link\tOwner" };
            foreach (var testRunMessage in testRunMessages)
            {
                string curDataFactoryName = testRunMessage["dataFactoryName"].ToString();
                if (!curDataFactoryName.Equals(dataFactoryName))
                {
                    continue;
                }

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
                    case 0:
                        missedMessageLines.Add(lineMessage);
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Not catch error message for dataset '{testRunMessage["datasetId"]}'.");
                        //The default foreground color for console is gray.
                        Console.ForegroundColor = ConsoleColor.Gray;
                        break;
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
                        userDefinedErrorLines.Add(lineMessage);
                        break;
                    case 8:
                        sameSourceLines.Add(lineMessage);
                        break;
                    case 9:
                        fileNotFoundLines.Add(lineMessage);
                        break;
                    case 10:
                        dynamicViewLines.Add(lineMessage);
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
            lines.AddRange(userDefinedErrorLines);
            lines.Add(string.Empty);
            lines.AddRange(sameSourceLines);
            lines.Add(string.Empty);
            lines.AddRange(fileNotFoundLines);
            lines.Add(string.Empty);
            lines.AddRange(dynamicViewLines);

            lines.Add(string.Empty);
            lines.AddRange(missedMessageLines);

            var folderPath = Path.Combine(CosmosViewErrorMessageOperation.RootFolderPath, dataFactoryName);
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }

            File.WriteAllLines(Path.Combine(folderPath, @"messageLines.tsv"), lines);
            File.WriteAllText(Path.Combine(folderPath, @"details_attach.json"), detailsJArray.ToString());
        }

        private static ViewFileInfo GetViewFileInfo(HashSet<ViewFileInfo> viewFileInfos, string viewName, string version)
        {
            ViewFileInfo viewFileInfo = null;
            var selectedViewFileInfos = viewFileInfos.Where(v => v.ViewName.Equals(viewName) && v.Version.Equals(version)).ToArray();

            if (selectedViewFileInfos.Length == 1)
            {
                viewFileInfo = selectedViewFileInfos[0];
            }
            else
            {
                //throw new FileNotFoundException($"There is no selected view file info for view name '{viewName}'");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"There is {selectedViewFileInfos.Length} selected view file info for view name '{viewName}'");
                Console.ForegroundColor = ConsoleColor.Gray;
            }

            return viewFileInfo;
        }

        private static IEnumerable<ViewFileInfo> GetViewNames(string viewFileFolderPath)
        {
            var jsonFilePaths = Directory.GetFiles(viewFileFolderPath, "*.json", SearchOption.AllDirectories);
            foreach (var jsonFilePath in jsonFilePaths)
            {
                JObject json = JObject.Parse(File.ReadAllText(jsonFilePath));
                if (!json.ContainsKey("version") ||
                    !json.ContainsKey("name"))
                {
                    continue;
                }

                // For the template output view file, the file name does not need to be defined in the json file,
                // View file name is just the same as json file name
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(jsonFilePath);
                var viewFilePath = Path.Combine(Path.GetDirectoryName(jsonFilePath), fileNameWithoutExtension + ".view");
                if (File.Exists(viewFilePath))
                {
                    yield return new ViewFileInfo
                    {
                        ViewName = json["name"].ToString(),
                        // We add a leading character "v" .
                        Version = @"v" + json["version"].ToString(),
                        JsonFilePath = jsonFilePath,
                        ViewFilePath = viewFilePath,
                    };
                }


                if (!json.ContainsKey("fileName") ||
                    !json["fileName"].ToString().EndsWith(@".view"))
                {
                    continue;
                }

                viewFilePath = Path.Combine(Path.GetDirectoryName(jsonFilePath), json["fileName"].ToString());
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

            string error = string.Empty;
            foreach (var message in messages)
            {
                var userDefinedErrorMatch = UserDefinedErrorMatchRegex.Match(message.ToString());
                if (userDefinedErrorMatch.Success)
                {
                    var curError = userDefinedErrorMatch.Result("${error}").ToString();
                    if (string.IsNullOrEmpty(error) || !error.Equals(curError))
                    {
                        errorMessage = message.ToString();
                        error = curError;
                    }
                }
            }

            if (!string.IsNullOrEmpty(error))
            {
                errorContent = error;
                return 7;
            }

            string fileName = string.Empty;
            foreach (var message in messages)
            {
                var sameSourceMatch = SameSourceMatchRegex.Match(message.ToString());
                if (sameSourceMatch.Success)
                {
                    var curFileName = sameSourceMatch.Result("${file}").ToString();
                    if (string.IsNullOrEmpty(fileName) || !fileName.Equals(curFileName))
                    {
                        errorMessage = message.ToString();
                        fileName = curFileName;
                    }
                }
            }

            if (!string.IsNullOrEmpty(fileName))
            {
                errorContent = fileName;
                return 8;
            }

            string filePath = string.Empty;
            foreach (var message in messages)
            {
                var fileNotFoundMatch = FileNotFoundMatchRegex.Match(message.ToString());
                if (fileNotFoundMatch.Success)
                {
                    var curFilePath = fileNotFoundMatch.Result("${file}").ToString();
                    if (string.IsNullOrEmpty(fileName) || !fileName.Equals(curFilePath))
                    {
                        errorMessage = message.ToString();
                        filePath = curFilePath;
                    }
                }
            }

            if (!string.IsNullOrEmpty(filePath))
            {
                errorContent = filePath;
                return 9;
            }

            string resource = string.Empty;
            foreach (var message in messages)
            {
                var fileNotFoundMatch = DynamicViewMatchRegex.Match(message.ToString());
                if (fileNotFoundMatch.Success)
                {
                    var curResource = fileNotFoundMatch.Result("${resource}").ToString();
                    if (string.IsNullOrEmpty(fileName) || !fileName.Equals(curResource))
                    {
                        errorMessage = message.ToString();
                        resource = curResource;
                    }
                }
            }

            if (!string.IsNullOrEmpty(resource))
            {
                errorContent = resource;
                return 10;
            }


            errorContent = @"";
            return 0;
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
