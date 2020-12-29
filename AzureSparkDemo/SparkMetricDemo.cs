namespace AzureSparkDemo
{
    using AzureLib.KeyVault;
    using AzureSparkDemo.Clients;
    using AzureSparkDemo.Models;
    using CommonLib.IDEAs;
    using Microsoft.Azure.Databricks.Client;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;

    /// <summary>
    /// Class SparkMetricsCorrectness.
    /// </summary>
    public class SparkMetricDemo
    {
        public static void MainMethod()
        {
            ProcessMetricsCorrectnessDemo();
        }

        public const string testDateString = "@testDate";

        private static string testRunName = @"DemoTestRunName";
        private static string testRunId = @"DemoTestRunId";

        private static void ProcessMetricsCorrectnessDemo()
        {
            string sparkTestContentJsonFilePath =
                @"D:\data\company_work\M365\IDEAs\datacop\SparkWorker\TestContents\a5e052c3-4b8a-4d41-9259-74c9567c28fd.json";

            Configuration configuration = new Configuration();
            configuration.LoadConfiguration();
            Console.WriteLine(configuration.TenantId);

            SparkMetricsTestContent testContent =
                JsonConvert.DeserializeObject<SparkMetricsTestContent>(File.ReadAllText(sparkTestContentJsonFilePath));
            var results = ProcessMetricsCorrectnessTest(
                configuration.DataLakeClient,
                testContent,
                configuration.SparkClient,
                configuration.SparkClientSettings,
                new CancellationToken());
            foreach (var result in results)
            {
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(result)));
            }
        }

        /// <summary>
        /// Processes the metrics correctness test.
        /// </summary>
        /// <param name="dataLakeClient">The data lake client.</param>
        /// <param name="testContent">Content of the test.</param>
        /// <param name="sparkClient">The spark client.</param>
        /// <param name="sparkClientSettings">The spark client settings.</param>
        /// <returns>SparkMetricsTestResult.</returns>
        /// <exception cref="NotSupportedException">ComparisonType {testContent.ComparisonType}</exception>
        private static List<MetricsTestResult> ProcessMetricsCorrectnessTest(
            DataLakeClient dataLakeClient,
            SparkMetricsTestContent testContent,
            SparkClient sparkClient,
            SparkClientSettings sparkClientSettings,
            CancellationToken cancellationToken)
        {

            MetricValues metricValues = null;
            switch (testContent.ComparisonType)
            {
                case ComparisonType.DayOverDay:
                case ComparisonType.WeekOverWeek:
                case ComparisonType.MonthOverMonth:
                case ComparisonType.YearOverYear:
                    metricValues = GetMetricValues(
                        dataLakeClient,
                        testContent,
                        sparkClient,
                        sparkClientSettings,
                        cancellationToken);
                    break;
                case ComparisonType.VarianceToTarget:
                    var metricValue = GetMetricValue(
                        testContent,
                        sparkClient,
                        sparkClientSettings,
                        testContent.GetCurrentStreamPath(),
                        dataLakeClient,
                        cancellationToken);
                    testContent.NotebookParameters["cmdText"] = testContent.NotebookParameters["targetCmdText"];
                    var targetValue = GetMetricValue(
                        testContent,
                        sparkClient,
                        sparkClientSettings,
                        testContent.TargetStreamPath != null ? testContent.GetCurrentTargetStreamPath() : testContent.GetCurrentStreamPath(),
                        dataLakeClient,
                        cancellationToken);

                    metricValues = new MetricValues
                    {
                        Baseline = targetValue,
                        Current = metricValue
                    };
                    break;
                default:
                    throw new NotSupportedException($"ComparisonType {testContent.ComparisonType} not supported for SparkWorker");
            }

            var results = new List<MetricsTestResult>();

            foreach (var threshold in testContent.Thresholds)
            {
                var result = new MetricsTestResult
                {
                    ComparisonType = testContent.ComparisonType,
                    Date = testContent.Date,
                    BaselineMetricValue = metricValues.Baseline[threshold.Name],
                    MetricValue = metricValues.Current[threshold.Name],
                    LowerBoundThreshold = threshold.LowerBound,
                    UpperBoundThreshold = threshold.UpperBound,
                    PercentDiff = MetricValues.ComputePercentDiff(metricValues, threshold.Name),
                    PreviousDate = testContent.GetPreviousDate(),
                    TestName = testRunName,
                    TestRunId = testRunId,
                    MetricName = threshold.Name
                };

                results.Add(result);
            }

            return results;
        }


        /// <summary>
        /// Gets the metric values.
        /// </summary>
        /// <param name="dataLakeClient">The data lake client.</param>
        /// <param name="testContent">Content of the test.</param>
        /// <param name="sparkClient">The spark client.</param>
        /// <param name="sparkClientSettings">The spark client settings.</param>
        /// <returns>MetricValues.</returns>
        private static MetricValues GetMetricValues(
            DataLakeClient dataLakeClient,
            SparkMetricsTestContent testContent,
            SparkClient sparkClient,
            SparkClientSettings sparkClientSettings,
            CancellationToken cancellationToken)
        {
            string currentPath = testContent.GetCurrentStreamPath();
            string previousPath = testContent.GetPreviousStreamPath();

            var current = GetMetricValue(
                testContent,
                sparkClient,
                sparkClientSettings,
                currentPath,
                dataLakeClient,
                cancellationToken);
            var previous = GetMetricValue(
                testContent,
                sparkClient,
                sparkClientSettings,
                previousPath,
                dataLakeClient,
                cancellationToken);

            var metricValues = new MetricValues
            {
                Baseline = previous,
                Current = current
            };

            return metricValues;
        }

        /// <summary>
        /// Gets the metric value.
        /// </summary>
        /// <param name="testContent">Content of the test.</param>
        /// <param name="sparkClient">The spark client.</param>
        /// <param name="sparkClientSettings">The spark client settings.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="dataLakeClient">The data lake client.</param>
        /// <returns>System.Nullable&lt;System.Double&gt;.</returns>
        /// <exception cref="InvalidOperationException">Stream does not exist : {stream}</exception>
        private static IDictionary<string, double> GetMetricValue(
            SparkMetricsTestContent testContent,
            SparkClient sparkClient,
            SparkClientSettings sparkClientSettings,
            string stream,
            DataLakeClient dataLakeClient,
            CancellationToken cancellationToken)
        {

            if (!dataLakeClient.CheckExists(testContent.GetDatalakeStore(), stream))
            {
                throw new FileNotFoundException($"Stream does not exist : {stream}");
            }

            var sparkRequest = new SparkClientRequest
            {
                NodeType = sparkClientSettings.NodeType,
                NumWorkersMin = sparkClientSettings.NumWorkersMin,
                NumWorkersMax = sparkClientSettings.NumWorkersMax,
                CostPerNode = GetCostPerNode(sparkClientSettings.NodeTypes, sparkClientSettings.NodeType),
                Libraries = sparkClientSettings.Libraries,
                NotebookPath = testContent.NotebookPath,
                NotebookParameters = testContent.NotebookParameters ?? new Dictionary<string, string>(),
                TestRunId = testRunId,
                TimeoutSeconds = sparkClientSettings.TimeoutSeconds
            };

            Console.WriteLine($"Running notebook={testContent.NotebookPath} for DataLakeStore={testContent.GetDatalakeStore()}, Path={stream}");
            var mountPoint = GetMountPoint(testContent.GetDatalakeStore(), stream);
            Console.WriteLine($"Running notebook={testContent.NotebookPath} for mountPoint={mountPoint}");

            string streamPath = mountPoint;

            // Disable this function
            //if (testContent.ConvertToParquet)
            //{
            //    var parquetFile = messageTag.MountPointToParquetFile[mountPoint];
            //    Console.WriteLine($"Running notebook={testContent.NotebookPath} using parquetFile={parquetFile}");
            //    streamPath = parquetFile;
            //}
            //else
            //{
            //    streamPath = mountPoint;
            //}

            sparkRequest.NotebookParameters["streamPath"] = streamPath;

            Console.WriteLine($"Notebook parameters : {string.Join(", ", sparkRequest.NotebookParameters.Select(t => t.Key + "=" + t.Value))}");

            // Log request to OMS

            var response = sparkClient.RunNotebook(sparkRequest, cancellationToken);
            response.TestRunId = testRunId;

            if (response.IsRunSuccess())
            {
                // For format reference see:
                // https://pandas.pydata.org/pandas-docs/version/0.24.2/reference/api/pandas.DataFrame.to_json.html
                var resultDataFrame = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, double>>>(response.RunOutput);
                var resultDictionary = new Dictionary<string, double>();
                foreach (var pair in resultDataFrame)
                {
                    // pair.Value is the column name
                    if (pair.Value == null || pair.Value.Count == 0)
                    {
                        throw new InvalidOperationException("Result does not contain any rows");
                    }

                    // We take the first row only
                    resultDictionary.Add(pair.Key, pair.Value.First().Value);
                }
                return resultDictionary;
            }
            else
            {
                throw new Exception($"Error getting metric. TestRun = {testRunId}, Spark job {response?.Run?.RunId} failed");
            }
        }

        /// <summary>
        /// Gets the cost per node.
        /// </summary>
        /// <param name="nodeTypes">The node types.</param>
        /// <param name="nodeType">Type of the node.</param>
        /// <returns>System.Double.</returns>
        /// <exception cref="ArgumentException">Could not find node type {nodeType}</exception>
        public static double GetCostPerNode(List<NodeTypeExpression> nodeTypes, string nodeType)
        {
            var record = nodeTypes.Where(t => t.type == nodeType).FirstOrDefault();
            if (record == null)
            {
                throw new ArgumentException($"Could not find node type {nodeType}");
            }
            return double.Parse(record.cost);
        }

        /// <summary>
        /// Gets the mount point.
        /// </summary>
        /// <param name="datalakeStore">The datalake store.</param>
        /// <param name="streamPath">The stream path.</param>
        /// <returns>string.</returns>
        /// <exception cref="ArgumentNullException">datalakeStore
        /// or
        /// streamPath</exception>
        /// <exception cref="InvalidOperationException">Expected 3 tokens in string {datalakeStore}</exception>
        public static string GetMountPoint(string datalakeStore, string streamPath)
        {
            if (string.IsNullOrWhiteSpace(datalakeStore))
            {
                throw new ArgumentNullException(nameof(datalakeStore));
            }

            if (string.IsNullOrWhiteSpace(streamPath))
            {
                throw new ArgumentNullException(nameof(streamPath));
            }

            var tokens = datalakeStore.Split('.');

            if (tokens.Length != 3)
            {
                throw new InvalidOperationException($"Expected 3 tokens in string {datalakeStore}");
            }

            return $"/mnt/{tokens[0]}/{streamPath.TrimStart('/')}";
        }
    }
}

