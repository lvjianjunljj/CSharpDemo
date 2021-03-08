namespace CSharpDemo.IDEAs
{
    using CSharpDemo.Azure;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Security;
    using System.Text.RegularExpressions;
    using System.Threading;

    class CloudScopeOperation
    {
        private static string CloudScopeSubmitJobUrl = @"http://cloudscope-prod-gdpr.asecloudscopeprod.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/submitJob?workloadQueueName=Gdpr";
        private static string CloudScopeJobInfoUrlFormat = @"http://cloudscope-prod-gdpr.asecloudscopeprod.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/jobInfo/{0}";
        private static string TenantId = @"cdc5aeea-15c5-4db6-b079-fcadd2505dc2";
        private static string ClientId = @"8e2888f4-cebf-47ad-9811-53a584413523";
        private static string ClientSecret = @"";
        private static string Resource = @"api://9576eb06-ef2f-4f45-a131-e33d0e7ffb00";

        public static void MainMethod()
        {
            //SubmitFailedCosmosjob();
            UpdateJobStatuses();
        }

        private static void SubmitFailedCosmosjob()
        {
            // Need to run CosmosDemo.FunctionDemo.DownloadViewScripts() first.
            Console.WriteLine(@"Sneding submit job requests start...");

            var testRunMessagesPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\allTestRuns.json";
            var jobStatusesPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\allJobStatuses.json";
            var viewsFolder = @"D:\IDEAs\repos\CosmosViewMonitor\cosmos_views";

            JArray testRunMessages = JArray.Parse(File.ReadAllText(testRunMessagesPath));
            JArray datasetViewDict = JArray.Parse(File.ReadAllText(Path.Combine(viewsFolder, @"datasetViewDict.json")));

            Dictionary<string, string> viewFilePaths = new Dictionary<string, string>();
            foreach (var datasetView in datasetViewDict)
            {
                var viewFilePath = Path.Combine(viewsFolder, datasetView["viewFileName"].ToString());
                viewFilePaths.Add(datasetView["datasetId"].ToString(), viewFilePath);
            }

            // PPE resource: api://ead06413-cb7c-408e-a533-2cdbe58bf3a6
            // Prod resource； api://9576eb06-ef2f-4f45-a131-e33d0e7ffb00
            var gdprToken = AzureActiveDirectoryToken.GetAccessTokenV1Async(TenantId, ClientId, ClientSecret, Resource).Result;

            var jobStatuses = new JArray();
            foreach (var testRunMessage in testRunMessages)
            {
                var datasetId = testRunMessage["datasetId"].ToString();
                var testDateStrs = JArray.Parse(testRunMessage["testDates"].ToString());
                var jobStatus = new JObject();
                jobStatus["datasetId"] = datasetId;
                jobStatus["testDates"] = testDateStrs;
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

                    //var scriptParams = new JObject();
                    //var parameters = GetParameters(cosmosScriptContent);
                    //foreach (var parameter in parameters)
                    //{
                    //    scriptParams[parameter.Key] = parameter.Value;
                    //}
                    //var response = SendSubmitJobRequest(viewFilePaths[datasetId], gdprToken, scriptParams);


                    // Cosmos view is like a function and cosmosScriptContent is to call this function with some input, we need to submit a process to adla.
                    // So we cannot just submit a cosmos view to adla.
                    var response = SendSubmitJobRequest(scriptToCompile, gdprToken);
                    Console.WriteLine($"SendSubmitJobRequest response: '{response}'");

                    jobIdDict.Add(testDateStr.ToString(), response.Trim('"'));
                    jobIds.Add(response.Trim('"'));
                }

                jobStatus["jobIds"] = jobIds;
                jobStatuses.Add(jobStatus);
            }

            File.WriteAllText(jobStatusesPath, jobStatuses.ToString());
            Console.WriteLine(@"Sneding submit job requests end...");
        }

        private static void UpdateJobStatuses()
        {
            Console.WriteLine(@"Writing job statuses start...");
            var jobStatusesPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\allJobStatuses.json";
            JArray jobStatuses = JArray.Parse(File.ReadAllText(jobStatusesPath));
            var gdprToken = AzureActiveDirectoryToken.GetAccessTokenV1Async(TenantId, ClientId, ClientSecret, Resource).Result;

            foreach (var jobStatus in jobStatuses)
            {
                var jobIds = JArray.Parse(jobStatus["jobIds"].ToString());
                JArray responses = new JArray();
                foreach (var jobId in jobIds)
                {
                    var response = SendJobInfoRequest(jobId.ToString(), gdprToken);
                    var json = JObject.Parse(response);
                    responses.Add(json);
                }
                jobStatus["responses"] = responses;
            }
            File.WriteAllText(jobStatusesPath, jobStatuses.ToString());
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
                {"PipelineRunId", Guid.NewGuid().ToString()},
                {"Compression", "false"},
                {"FriendlyName", "Submit Scope Example by api(jianjlv)"},
                {"Priority", "1000"},
                {"TokenAllocation","0"},
                {"VirtualClusterPercentAllocation","0"},
                {"NebulaCommandLineArgs"," -on adl "},
                {"ScriptParams", scriptParams.ToString()}
            };

            return SendSubmitJobRequest(token, values);
        }

        private static string SendSubmitJobRequest(string scriptStr, string token)
        {
            var values = new Dictionary<string, string>
            {
                { "Script", scriptStr},
                {"PipelineRunId", Guid.NewGuid().ToString()},
                {"Compression", "false"},
                {"FriendlyName", "Submit Scope Example by api(jianjlv)"},
                {"Priority", "1000"},
                {"TokenAllocation","0"},
                {"VirtualClusterPercentAllocation","0"},
                {"NebulaCommandLineArgs"," -on adl "},
            };

            return SendSubmitJobRequest(token, values);
        }

        private static string SendSubmitJobRequest(string token, Dictionary<string, string> values)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, CloudScopeSubmitJobUrl)
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
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, string.Format(CloudScopeJobInfoUrlFormat, jobId));

            var response = client.SendAsync(request).Result;

            return response.Content.ReadAsStringAsync().Result;
        }
    }
}
