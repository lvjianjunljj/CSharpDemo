namespace KenshoDemo
{
    using AzureLib.KeyVault;
    using KenshoDemo.Models;
    using KenshoDemo.QueryModels;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.IO;
    using System.Net;

    public class QueryDetectResultDemo
    {
        private static string kenshoTestContentJsonFilePath;
        public static void MainMethod()
        {
            Initialize(@"datacop-prod");
            kenshoTestContentJsonFilePath = @"D:\data\company_work\M365\IDEAs\datacop\Kensho\kensho_test_content\demo_1.json";
            CheckForAnomalies();

            kenshoTestContentJsonFilePath = @"D:\data\company_work\M365\IDEAs\datacop\Kensho\kensho_test_content\demo_2.json";
            CheckForAnomalies();
        }

        private static void CheckForAnomalies()
        {
            Console.WriteLine("Staet anomalies query...");
            var content = JsonConvert.DeserializeObject<KenshoTestContent>(File.ReadAllText(kenshoTestContentJsonFilePath));

            int pageNum = 0;
            int pageSize = 1000;
            List<KenshoDetectAnomaly> anomalies = GetAnomalies(content.MetricGuid, content.TestDate, content.ConfigId, pageNum, pageSize);

            Console.WriteLine("Outout anomalies query result: ");
            foreach (var anomaly in anomalies)
            {
                Console.WriteLine(anomaly);
            }
        }

        /// <summary>
        /// REtrieves any anomalies found on a given date.
        /// </summary>
        /// <param name="metricGuid">The metric we are processing.</param>
        /// <param name="startTime">The date we are processing.</param>
        /// <param name="configId">The detect config id in Kensho.</param>
        /// <param name="pageNum">The page of anomalies we are currently on.</param>
        /// <param name="pageSize">Max number of anomalies to retrieve.</param>
        /// <returns>A list of anomalies found.</returns>
        private static List<KenshoDetectAnomaly> GetAnomalies(Guid metricGuid, DateTime startTime, Guid configId, int pageNum, int pageSize)
        {
            KenshoQueryDetectResultAnomaliesRequest request = new KenshoQueryDetectResultAnomaliesRequest()
            {
                MetricGuid = metricGuid,
                DetectConfigGuid = configId,
                StartTime = startTime,
                EndTime = startTime.AddDays(1),
                PageNum = pageNum,
                PageSize = pageSize,
                Filter = new KenshoQueryFilter()
                {
                    SeverityFilter = new KenshoQuerySeverityFilter()
                    {
                        // For now, no one is likely to want anything different for this, so we will just hard code these values.
                        // If more flexibility is required later, we will need to modify KenshoTestContent to include a direction and
                        // we will possible want to change the types of min and max severity to be strings instead of the old ints.
                        MinSeverity = "High",
                        MaxSeverity = "High",
                        AnomalyDetectorDirection = "Both",
                    },
                },
            };

            //Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(request)));
            KenshoQueryDetectResultAnomaliesResponse response = SendQueryDetectResultAnomaliesRequestAsync(request);
            if (response.StatusCode != HttpStatusCode.OK)
            {
                throw new ProtocolViolationException($"query_detect_results_anomalies returned error {response.StatusCode}, message {response.ContentOrErrorMessage}");
            }

            return response.AnomalySetList;
        }

        private static KenshoQueryDetectResultAnomaliesResponse SendQueryDetectResultAnomaliesRequestAsync(KenshoQueryDetectResultAnomaliesRequest request)
        {
            KenshoQueryDetectResultAnomaliesResponse response;

            KenshoBaseResponse responseMessage = kenshoProvider.SendRequestAsync(JsonConvert.SerializeObject(request), QueryDetectResultAmomalies).Result;
            if (responseMessage.StatusCode == HttpStatusCode.OK)
            {
                response = new KenshoQueryDetectResultAnomaliesResponse()
                {
                    StatusCode = HttpStatusCode.OK,
                    AnomalySetList = JsonConvert.DeserializeObject<List<KenshoDetectAnomaly>>(responseMessage.ContentOrErrorMessage),
                };
            }
            else
            {
                response = new KenshoQueryDetectResultAnomaliesResponse()
                {
                    StatusCode = responseMessage.StatusCode,
                    ContentOrErrorMessage = responseMessage.ContentOrErrorMessage,
                };
            }

            return response;
        }

        private static void Initialize(string keyVaultName)
        {
            KeyVaultName = keyVaultName;
            secretProvider = KeyVaultSecretProvider.Instance;

            KenshoSettings kenshoSettings = new KenshoSettings()
            {
                Environment = GetRequiredSetting<string>("Environment"),
                UIURL = GetRequiredSetting<string>("KenshoUIURL"),
                APIURL = GetRequiredSetting<string>("KenshoAPIURL"),
                CosmosVC = GetRequiredSetting<string>("KenshoCosmosVC"),
                APITenantKey = GetSecret(GetRequiredSetting<string>("KenshoSecretName")),
                BackendAPITenantKey = GetSecret(GetRequiredSetting<string>("KenshoBackendSecretName")),
                BackendAPIURL = GetRequiredSetting<string>("KenshoBackendAPIURL"),
            };

            kenshoProvider = new KenshoProvider(kenshoSettings);
        }

        /// <summary>
        /// Gets the required setting.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="settingName">Name of the setting.</param>
        /// <returns>T.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException">Not able to load required configuration {settingName}</exception>
        private static T GetRequiredSetting<T>(string settingName)
        {
            try
            {
                string settingValue = ConfigurationManager.AppSettings[settingName] ?? throw new ArgumentNullException($"{settingName} does not exist in the App.config");
                return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFromString(settingValue);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Not able to load required configuration {settingName}", ex);
            }
        }

        /// <summary>
        /// Gets the secret.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>System.String.</returns>
        private static string GetSecret(string name)
        {
            return secretProvider.GetSecretAsync(KeyVaultName, name).Result;
        }

        private static ISecretProvider secretProvider;
        private static string KeyVaultName;
        private static KenshoProvider kenshoProvider;
        private static string QueryDetectResultAmomalies = "smartalert/query_detect_result_anomalies";
    }
}
