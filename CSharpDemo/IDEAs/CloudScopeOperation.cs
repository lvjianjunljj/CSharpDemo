namespace CSharpDemo.IDEAs
{
    using AzureLib.KeyVault;
    using CSharpDemo.Azure;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text.RegularExpressions;

    class CloudScopeOperation
    {
        private static string Service = "";
        private static string VirtualCluster = "";
        private static string WorkloadQueueName = "";
        private static readonly string CloudScopeSubmitJobUrlFormat = @"http://cloudscope-prod-{0}.asecloudscopeprod.p.azurewebsites.net/api/virtualCluster/{1}/submitJob?workloadQueueName={2}";
        private static readonly string CloudScopeJobInfoUrlFormat = @"http://cloudscope-prod-{0}.asecloudscopeprod.p.azurewebsites.net/api/virtualCluster/{1}/jobInfo/{2}";
        private static readonly string TestRunMessagesPath = Path.Combine(CosmosViewErrorMessageOperation.RootFolderPath, @"allTestRuns.json");
        private static readonly string JobStatusesPath = Path.Combine(CosmosViewErrorMessageOperation.RootFolderPath, @"allJobStatuses_prod.json");

        private static string Token;

        public static void MainMethod()
        {
            Initialize(@"datacop-prod");
            SubmitFailedCosmosjob();
            //UpdateJobStatuses();
        }

        private static void Initialize(string keyVaultName)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            // PPE resource: api://ead06413-cb7c-408e-a533-2cdbe58bf3a6
            // Prod resource； api://9576eb06-ef2f-4f45-a131-e33d0e7ffb00

            // For ideas-prod-c14
            //Service = @"gdpr";
            //VirtualCluster = @"ideas-prod-c14";
            //WorkloadQueueName = @"gdpr";
            //string tenantId = @"cdc5aeea-15c5-4db6-b079-fcadd2505dc2";
            //string resource = @"api://9576eb06-ef2f-4f45-a131-e33d0e7ffb00";
            //string clientId = secretProvider.GetSecretAsync(keyVaultName, "GdprClientId").Result;
            //string clientSecret = secretProvider.GetSecretAsync(keyVaultName, "GdprClientSecret").Result;
            
            // For ideas-prod-build-c14
            Service = @"pls";
            VirtualCluster = @"ideas-prod-build-c14";
            WorkloadQueueName = @"build"; // The default value is "build".
            string tenantId = @"72f988bf-86f1-41af-91ab-2d7cd011db47";
            string resource = @"api://d42d6163-88e5-4339-bbb3-28ec2fb3c574";
            string clientId = secretProvider.GetSecretAsync(keyVaultName, "PlsAadAuthAppId").Result;
            string clientSecret = secretProvider.GetSecretAsync(keyVaultName, "PlsAadAuthAppSecret").Result;

            Token = AzureActiveDirectoryToken.GetAccessTokenV1Async(tenantId, clientId, clientSecret, resource).Result;
        }

        private static void SubmitFailedCosmosjob()
        {
            // Need to run CosmosDemo.FunctionDemo.DownloadViewScripts() first.
            Console.WriteLine(@"Sending submit job requests start...");

            JArray testRunMessages = JArray.Parse(File.ReadAllText(TestRunMessagesPath));
            var jobStatuses = new JArray();
            foreach (var testRunMessage in testRunMessages)
            {
                var testDateStrs = JArray.Parse(testRunMessage["testDates"].ToString());
                var jobStatus = new JObject
                {
                    ["datasetId"] = testRunMessage["datasetId"].ToString(),
                    ["datasetName"] = testRunMessage["datasetName"].ToString(),
                    ["testDates"] = testDateStrs,
                };
                var jobIds = new JArray();
                var jobIdDict = new Dictionary<string, string>();
                foreach (var testDateStr in testDateStrs)
                {
                    if (jobIdDict.ContainsKey(testDateStr.ToString()))
                    {
                        jobIds.Add(jobIdDict[testDateStr.ToString()]);
                        continue;
                    }
                    var testDate = DateTime.Parse(testDateStr.ToString());
                    var nextDate = testDate.AddDays(1);

                    var scriptToCompile = testRunMessage["cosmosScriptContent"].ToString()
                    .Replace("@@TestDate@@", "\"" + testDate.ToString() + "\"")
                    .Replace("@@NextDate@@", "\"" + nextDate.ToString() + "\"");
                    scriptToCompile = scriptToCompile.Replace("PARAMS()", ""); // Remove empty PARAMS clause.

                    // Cosmos view is like a function and cosmosScriptContent is to call this function with some input, we need to submit a process to adla.
                    // So we cannot just submit a cosmos view to adla.
                    var response = SendSubmitJobRequest(scriptToCompile, Token);
                    Console.WriteLine($"SendSubmitJobRequest response: '{response}'");

                    jobIdDict.Add(testDateStr.ToString(), response.Trim('"'));
                    jobIds.Add(response.Trim('"'));
                }

                jobStatus["jobIds"] = jobIds;
                jobStatuses.Add(jobStatus);
            }

            File.WriteAllText(JobStatusesPath, jobStatuses.ToString());
            Console.WriteLine(@"Sending submit job requests end...");
        }

        private static void UpdateJobStatuses()
        {
            Console.WriteLine(@"Writing job statuses start...");
            JArray jobStatuses = JArray.Parse(File.ReadAllText(JobStatusesPath));
            JArray newJobStatuses = new JArray();
            foreach (var jobStatus in jobStatuses)
            {
                var jobIds = JArray.Parse(jobStatus["jobIds"].ToString());
                var newJobStatus = new JObject
                {
                    ["datasetId"] = jobStatus["datasetId"],
                    ["datasetName"] = jobStatus["datasetName"].ToString(),
                    ["testDates"] = jobStatus["testDates"],
                    ["jobIds"] = jobIds
                };

                var states = new JArray();
                var errors = new JArray();

                foreach (var jobId in jobIds)
                {
                    var response = SendJobInfoRequest(jobId.ToString(), Token);
                    var json = JObject.Parse(response);
                    var state = json["CosmosState"].ToString().Equals("None") ? json["CloudScopeSubmissionState"] : json["CosmosState"];
                    var error = string.IsNullOrEmpty(json["Error"].ToString()) || json["Error"].ToString().Equals("Intermittent CloudScope Failure") ?
                        json["CloudScopeSubmissionError"] : json["Error"];
                    states.Add(state);
                    errors.Add(error);
                }
                newJobStatus["states"] = states;
                newJobStatus["errors"] = errors;

                newJobStatuses.Add(newJobStatus);
            }
            File.WriteAllText(JobStatusesPath, newJobStatuses.ToString());
            Console.WriteLine(@"Writing job statuses end...");
        }

        private static Dictionary<string, string> GetParameters(string cosmosScriptContent)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Regex cosmosViewParametersMatchRegex = new Regex(@"PARAMS\((?<parametersString>.*)\);OUTPUT");

            var cosmosViewParametersMatch = cosmosViewParametersMatchRegex.Match(cosmosScriptContent.ToString());
            if (cosmosViewParametersMatch.Success)
            {
                var parametersString = cosmosViewParametersMatch.Result("${parametersString}").ToString();
                if (string.IsNullOrEmpty(parametersString.Trim()))
                {
                    return parameters;
                }
                var parameterSplits = parametersString.Split(',');
                foreach (var parameterString in parameterSplits)
                {
                    var keyValueSplits = parameterString.Split('=');
                    parameters.Add(keyValueSplits[0].Trim(), keyValueSplits[1].Trim());
                }
                return parameters;
            }

            throw new Exception($"Cannot get parameters string from cosmosScriptContent '{cosmosScriptContent}'");
        }

        private static string SendSubmitJobRequest(string scriptFilePath, string token, JObject scriptParams)
        {
            var scriptStr = File.ReadAllText(scriptFilePath);
            var values = new Dictionary<string, string>
            {
                { "Script", scriptStr},
                { "PipelineRunId", Guid.NewGuid().ToString()},
                { "Compression", "false"},
                { "FriendlyName", "Submit Scope Example by api(jianjlv)"},
                { "Priority", "1000"},
                { "TokenAllocation","0"},
                { "VirtualClusterPercentAllocation","0"},
                { "NebulaCommandLineArgs"," -on adl "},
                { "ScriptParams", scriptParams.ToString()}
            };

            return SendSubmitJobRequest(token, values);
        }

        private static string SendSubmitJobRequest(string scriptStr, string token)
        {
            var values = new Dictionary<string, string>
            {
                { "Script", scriptStr},
                { "PipelineRunId", Guid.NewGuid().ToString()},
                { "Compression", "false"},
                { "FriendlyName", "Submit Scope Example by api(jianjlv)"},
                { "Priority", "1000"},
                { "TokenAllocation","0"},
                { "VirtualClusterPercentAllocation", "0"},
                { "NebulaCommandLineArgs", " -on adl "}
            };

            return SendSubmitJobRequest(token, values);
        }

        private static string SendSubmitJobRequest(string token, Dictionary<string, string> values)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, string.Format(CloudScopeSubmitJobUrlFormat, Service, VirtualCluster, WorkloadQueueName))
            {
                Content = new FormUrlEncodedContent(values)
            };

            var response = client.SendAsync(request).Result;

            return response.Content.ReadAsStringAsync().Result;
        }

        private static string SendJobInfoRequest(string jobId, string token)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, string.Format(CloudScopeJobInfoUrlFormat, Service, VirtualCluster, jobId));

            var response = client.SendAsync(request).Result;

            return response.Content.ReadAsStringAsync().Result;
        }
    }
}
