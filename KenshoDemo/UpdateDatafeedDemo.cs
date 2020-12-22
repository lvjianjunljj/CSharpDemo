namespace KenshoDemo
{
    using AzureLib.KeyVault;
    using KenshoDemo.Models;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.Net;

    public class UpdateDatafeedDemo
    {
        public static void MainMethod()
        {
            Initialize(@"datacop-prod");
            Console.WriteLine(1234);
            Console.ReadLine();
        }

        /// <summary>
        /// This will update the properties of a datafeed in Kensho.
        /// </summary>
        /// <param name="kenshoData">The Kensho configuration data to update.</param>
        /// <param name="oldAdminGroups">The previous admins in our local config or from Kensho.</param>
        /// <param name="oldViewerGroups">The previous viewers in our local config or from Kensho.</param>
        /// <returns>KenshoData</returns>
        private static void UpdateDatafeed(KenshoData kenshoData, List<string> oldAdminGroups, List<string> oldViewerGroups)
        {
            // Create the update request based on the new Kensho properties set on the dataset.
            KenshoUpdateDatafeedRequest updateDatafeedRequest = new KenshoUpdateDatafeedRequest()
            {
                DatafeedId = kenshoData.DatafeedId.GetValueOrDefault(),
                DatafeedName = kenshoData.DatafeedName,
                DataStartFrom = kenshoData.DataStartFrom.GetValueOrDefault(),
                GracePeriodInSeconds = kenshoData.GracePeriodInSeconds.GetValueOrDefault(),
                DisplayColumns = kenshoData.DisplayColumns,
                Metrics = new List<string>(),
                ParameterList = kenshoData.ParameterList,
                ScheduleIngestionDelayInSeconds = kenshoData.ScheduleIngestionDelayInSeconds.GetValueOrDefault(),
            };

            KenshoUpdateDatafeedResponse updateDatafeedResponse = SendUpdateDatafeedRequestAsync(updateDatafeedRequest);
            if (updateDatafeedResponse.StatusCode != HttpStatusCode.OK)
            {
                throw new ProtocolViolationException($"update_datafeed call returned error {updateDatafeedResponse.StatusCode}, error message {updateDatafeedResponse.ContentOrErrorMessage}");
            }
        }

        /// <summary>
        /// This will update some of the properties in an existing Kensho datafeed.
        /// </summary>
        /// <param name="request">Aa add_datafeed_users request.</param>
        /// <returns>A create_datafeed response.</returns>
        private static KenshoUpdateDatafeedResponse SendUpdateDatafeedRequestAsync(KenshoUpdateDatafeedRequest request)
        {
            KenshoUpdateDatafeedResponse response;

            KenshoBaseResponse responseMessage = kenshoProvider.SendRequestAsync(JsonConvert.SerializeObject(request), UpdateDatafeedString).Result;
            response = new KenshoUpdateDatafeedResponse()
            {
                StatusCode = responseMessage.StatusCode == HttpStatusCode.NoContent ? HttpStatusCode.OK : responseMessage.StatusCode,
                ContentOrErrorMessage = responseMessage.ContentOrErrorMessage,
            };

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
        private static string UpdateDatafeedString = "api/update_datafeed";
    }
}
