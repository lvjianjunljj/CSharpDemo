namespace AzureSparkDemo
{
    using AzureLib.KeyVault;
    using AzureSparkDemo.Clients;
    using AzureSparkDemo.Models;
    using Microsoft.Azure.Databricks.Client;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.Linq;

    public class Configuration
    {
        public string TenantId;
        public string ClientId;
        public string ClientKey;


        /// <summary>
        /// Gets the data bricks base URL.
        /// </summary>
        /// <value>The data bricks base URL.</value>
        public Uri DataBricksBaseUrl { get; private set; }

        /// <summary>
        /// Gets the data bricks token.
        /// </summary>
        /// <value>The data bricks token.</value>
        public string DataBricksToken { get; private set; }


        /// <summary>
        /// Gets the libraries.
        /// </summary>
        /// <value>The libraries.</value>
        public List<Library> Libraries { get; private set; }


        /// <summary>
        /// Gets the type of the node.
        /// </summary>
        /// <value>The type of the node.</value>
        public string NodeType { get; private set; }
        /// <summary>
        /// Gets the spark number workers.
        /// </summary>
        /// <value>The spark number workers.</value>
        public int SparkNumWorkersMin { get; private set; }
        public int SparkNumWorkersMax { get; private set; }


        /// <summary>
        /// Gets the node types.
        /// </summary>
        /// <value>The node types.</value>
        public List<NodeTypeExpression> NodeTypes { get; set; }


        /// <summary>
        /// Gets the data lake client.
        /// </summary>
        /// <value>The data lake client.</value>
        public DataLakeClient DataLakeClient { get; private set; }

        /// <summary>
        /// Gets the spark client.
        /// </summary>
        /// <value>The spark client.</value>
        public SparkClient SparkClient { get; set; }

        /// <summary>
        /// Gets the spark client settings.
        /// </summary>
        /// <value>The spark client settings.</value>
        public SparkClientSettings SparkClientSettings { get; set; }

        public DatabricksClientWrapper DatabricksClientWrapper { get; set; }
        public int MaxDegreeOfParallelism { get; set; }
        public int TimeoutSeconds { get; set; }

        public Configuration(
            ISecretProvider secretProvider = null,
            DatabricksClientWrapper databricksClientWrapper = null,
            DataLakeClient dataLakeClient = null)
        {
            this.DataLakeClient = dataLakeClient;
            this.DatabricksClientWrapper = databricksClientWrapper;
        }

        /// <summary>
        /// Loads the configuration.
        /// </summary>
        public void LoadConfiguration()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            this.TenantId = this.GetRequiredSetting<string>("TenantId");
            this.ClientId = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppId").Result;
            this.ClientKey = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppSecret").Result;
            this.DataBricksBaseUrl = new Uri(secretProvider.GetSecretAsync("datacop-prod", "DataBricksUrl").Result);
            this.DataBricksToken = secretProvider.GetSecretAsync("datacop-prod", "DataBricksToken").Result;

            // Read libraries
            {
                var libraryExpressions = ReadLibraries();
                this.Libraries = libraryExpressions.Select(t => GetLibrary(t.path)).ToList();
            }


            this.NodeType = this.GetRequiredSetting<string>("NodeType");
            this.SparkNumWorkersMin = this.GetRequiredSetting<int>("SparkNumWorkersMin");
            this.SparkNumWorkersMax = this.GetRequiredSetting<int>("SparkNumWorkersMax");

            this.NodeTypes = ReadNodeTypes();

            this.MaxDegreeOfParallelism = this.GetRequiredSetting<int>("MaxDegreeOfParallelism");
            this.TimeoutSeconds = this.GetRequiredSetting<int>("TimeoutSeconds");

            this.DataLakeClient = this.DataLakeClient ?? new DataLakeClient(this.TenantId, this.ClientId, this.ClientKey);

            this.DatabricksClientWrapper = new DatabricksClientWrapper(DataBricksBaseUrl.OriginalString, DataBricksToken);
            this.SparkClient = new SparkClient(this.DatabricksClientWrapper);
            this.SparkClientSettings = new SparkClientSettings
            {
                Libraries = this.Libraries,
                NodeType = this.NodeType,
                NodeTypes = this.NodeTypes,
                ClientId = this.ClientId,
                ClientKey = this.ClientKey,
                NumWorkersMin = this.SparkNumWorkersMin,
                NumWorkersMax = this.SparkNumWorkersMax,
                MaxDegreeOfParallelism = this.MaxDegreeOfParallelism,
                TimeoutSeconds = this.TimeoutSeconds
            };

        }

        public static Library GetLibrary(string token)
        {
            var extension = token.Split('.').Last();
            switch (extension)
            {
                case "jar":
                    Console.WriteLine($"Using jar library {token}");
                    return new JarLibrary(token);
                case "egg":
                    {
                        Console.WriteLine($"Using egg library {token}");
                        var egg = new EggLibrary();
                        egg.Egg = token;
                        return egg;
                    }
                case "whl":
                    {
                        Console.WriteLine($"Using wheel library {token}");
                        var wheel = new WheelLibrary();
                        wheel.Wheel = token;
                        return wheel;
                    }
                default:
                    throw new InvalidOperationException($"Unrecognized library extension {extension}");
            }
        }

        /// <summary>
        /// Reads the node types.
        /// </summary>
        /// <returns>List&lt;nodeTypeExpression&gt;.</returns>
        /// <exception cref="System.ArgumentNullException">nodeTypes</exception>
        /// <exception cref="ArgumentNullException">nodeTypes</exception>
        public static List<NodeTypeExpression> ReadNodeTypes()
        {
            var section = ConfigurationManager.GetSection("nodeTypes");
            var items = section as List<NodeTypeExpression>;

            if (items.Count < 1 ||
                items.Any(t => string.IsNullOrWhiteSpace(t.type)))
            {
                throw new ArgumentNullException("nodeTypes");
            }
            foreach (var t in items)
            {
                Console.WriteLine($"Using node types : {t}");
            }
            return items;
        }

        /// <summary>
        /// Reads the node types.
        /// </summary>
        /// <returns>List&lt;nodeTypeExpression&gt;.</returns>
        /// <exception cref="System.ArgumentNullException">nodeTypes</exception>
        /// <exception cref="ArgumentNullException">nodeTypes</exception>
        public static List<LibraryExpression> ReadLibraries()
        {
            var section = ConfigurationManager.GetSection("libraries");
            var items = section as List<LibraryExpression>;

            if (items.Count < 1 ||
                items.Any(t => string.IsNullOrWhiteSpace(t.path)))
            {
                throw new ArgumentNullException("libraries");
            }
            foreach (var t in items)
            {
                Console.WriteLine($"Using library : {t}");
            }
            return items;
        }

        /// <summary>
        /// Gets the required setting.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="settingName">Name of the setting.</param>
        /// <returns>T.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException">Not able to load required configuration {settingName}</exception>
        protected T GetRequiredSetting<T>(string settingName)
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

    }
}
