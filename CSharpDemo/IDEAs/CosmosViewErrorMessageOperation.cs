namespace CSharpDemo.IDEAs
{
    using Newtonsoft.Json.Linq;
    using System;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;

    class CosmosViewErrorMessageOperation
    {
        public static void MainMethod()
        {
            // Need to run "CosmosDemo.FunctionDemo.DownloadViewScripts()" to upload all cosmos view file to git.
            CollectErrorMessage();
        }

        private static void CollectErrorMessage()
        {
            Console.WriteLine(@"Start CollectErrorMessage...");
            var testRunMessagesPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\allTestRuns.json";
            JArray testRunMessages = JArray.Parse(File.ReadAllText(testRunMessagesPath));

            var nameNotExistMatchRegex = new Regex(
                @"E_CSC_USER_INVALIDCSHARP_0103: C# error CS0103: The name '(?<name>.*)' does not exist in the current context");
            var namespaceNotExistMatchRegex = new Regex(
                @"E_CSC_USER_INVALIDCSHARP_0234: C# error CS0234: The type or namespace name '(?<namespace_1>.*)' does not exist in the namespace '(?<namespace_2>.*)'");
            var rowsetCloumnTypeMatchRegex = new Regex(
                @"E_CSC_USER_ROWSETCOLUMNTYPEMISMATCH: The view declared the return rowset with column '(?<type_1>.*)' of type '(?<type_2>.*)'");
            var outputCloumnTypeMatchRegex = new Regex(
                @"E_CSC_USER_INVALIDTYPEINOUTPUTTER: DefaultTextOutputter was used to output column '(?<type_1>.*)' of type '(?<type_2>.*)'");
            var expectedTokenMatchRegex = new Regex(
                            @"Correct the script syntax, using expected token\(s\) as a guide\.\.\.\. at token '(?<token>.*)'");


            int missCount = 0;
            foreach (var testRunMessage in testRunMessages)
            {
                Console.WriteLine(testRunMessage["datasetId"]);
                var messages = testRunMessage["messages"].ToArray();

                string name = string.Empty;
                foreach (var message in messages)
                {
                    //Console.WriteLine(message);
                    var nameNotExistMatch = nameNotExistMatchRegex.Match(message.ToString());
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
                    continue;
                }

                string namespace_1 = string.Empty;
                string namespace_2 = string.Empty;
                foreach (var message in messages)
                {
                    //Console.WriteLine(message);
                    var namespaceNotExistMatch = namespaceNotExistMatchRegex.Match(message.ToString());
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
                    continue;
                }

                string rowsetCloumnType_1 = string.Empty;
                string rowsetCloumnType_2 = string.Empty;
                foreach (var message in messages)
                {
                    //Console.WriteLine(message);
                    var rowsetCloumnTypeMatch = rowsetCloumnTypeMatchRegex.Match(message.ToString());
                    if (rowsetCloumnTypeMatch.Success)
                    {
                        var cur_rowsetCloumnType_1 = rowsetCloumnTypeMatch.Result("${rowsetCloumnType_1}").ToString();
                        var cur_rowsetCloumnType_2 = rowsetCloumnTypeMatch.Result("${rowsetCloumnType_2}").ToString();
                        if (string.IsNullOrEmpty(rowsetCloumnType_1) ||
                            string.IsNullOrEmpty(rowsetCloumnType_2) ||
                            !rowsetCloumnType_1.Equals(cur_rowsetCloumnType_1) ||
                            !rowsetCloumnType_2.Equals(cur_rowsetCloumnType_2))
                        {
                            rowsetCloumnType_1 = cur_rowsetCloumnType_1;
                            rowsetCloumnType_2 = cur_rowsetCloumnType_2;
                            Console.WriteLine($"rowsetCloumnType_1: {rowsetCloumnType_1}");
                        }
                    }
                }

                if (!string.IsNullOrEmpty(rowsetCloumnType_1) || !string.IsNullOrEmpty(rowsetCloumnType_2))
                {
                    continue;
                }

                string outputCloumnType_1 = string.Empty;
                string outputCloumnType_2 = string.Empty;
                foreach (var message in messages)
                {
                    //Console.WriteLine(message);
                    var outputCloumnTypeMatch = outputCloumnTypeMatchRegex.Match(message.ToString());
                    if (outputCloumnTypeMatch.Success)
                    {
                        var cur_outputCloumnType_1 = outputCloumnTypeMatch.Result("${outputCloumnType_1}").ToString();
                        var cur_outputCloumnType_2 = outputCloumnTypeMatch.Result("${outputCloumnType_2}").ToString();
                        if (string.IsNullOrEmpty(outputCloumnType_1) ||
                            string.IsNullOrEmpty(outputCloumnType_2) ||
                            !outputCloumnType_1.Equals(cur_outputCloumnType_1) ||
                            !outputCloumnType_2.Equals(cur_outputCloumnType_2))
                        {
                            outputCloumnType_1 = cur_outputCloumnType_1;
                            outputCloumnType_2 = cur_outputCloumnType_2;
                            Console.WriteLine($"outputCloumnType_1: {outputCloumnType_1}");
                        }
                    }
                }

                if (!string.IsNullOrEmpty(outputCloumnType_1) || !string.IsNullOrEmpty(outputCloumnType_2))
                {
                    continue;
                }

                string token = string.Empty;
                foreach (var message in messages)
                {
                    //Console.WriteLine(message);
                    var expectedTokenMatch = expectedTokenMatchRegex.Match(message.ToString());
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
                    continue;
                }

                missCount++;
                Console.WriteLine("``````````````````");
            }
            Console.WriteLine($"missCount: {missCount}");
            System.Console.WriteLine(@"End CollectErrorMessage...");

        }
    }
}
