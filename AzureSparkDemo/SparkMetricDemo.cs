namespace AzureSparkDemo
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;

    /// <summary>
    /// Class SparkMetricsCorrectness.
    /// </summary>
    public static class SparkMetricDemo
    {
        public static void MainMethod()
        {
            Console.WriteLine("Hellow Workd");
        }

        public const string testDateString = "@testDate";

        ///// <summary>
        ///// Processes the metrics correctness test.
        ///// </summary>
        ///// <param name="dataLakeClient">The data lake client.</param>
        ///// <param name="logger">The logger.</param>
        ///// <param name="testRun">The test run.</param>
        ///// <param name="testContent">Content of the test.</param>
        ///// <param name="sparkClient">The spark client.</param>
        ///// <param name="sparkClientSettings">The spark client settings.</param>
        ///// <param name="messageTag">The message tag.</param>
        ///// <returns>SparkMetricsTestResult.</returns>
        ///// <exception cref="NotSupportedException">ComparisonType {testContent.ComparisonType}</exception>
        //private static List<MetricsTestResult> ProcessMetricsCorrectnessTest(
        //    IDataLakeClient dataLakeClient,
        //    ILogger logger,
        //    TestRun testRun,
        //    SparkMetricsTestContent testContent,
        //    ISparkClient sparkClient,
        //    SparkClientSettings sparkClientSettings,
        //    SparkWorkerMessageTag messageTag,
        //    CancellationToken cancellationToken)
        //{

        //    MetricValues metricValues = null;
        //    switch (testContent.ComparisonType)
        //    {
        //        case ComparisonType.DayOverDay:
        //        case ComparisonType.WeekOverWeek:
        //        case ComparisonType.MonthOverMonth:
        //        case ComparisonType.YearOverYear:
        //            metricValues = GetMetricValues(
        //                dataLakeClient,
        //                logger,
        //                testRun,
        //                testContent,
        //                sparkClient,
        //                sparkClientSettings,
        //                messageTag,
        //                cancellationToken);
        //            break;
        //        case ComparisonType.VarianceToTarget:
        //            var metricValue = GetMetricValue(
        //                logger,
        //                testRun,
        //                testContent,
        //                sparkClient,
        //                sparkClientSettings,
        //                testContent.GetCurrentStreamPath(),
        //                dataLakeClient,
        //                messageTag,
        //                cancellationToken);
        //            testContent.NotebookParameters["cmdText"] = testContent.NotebookParameters["targetCmdText"];
        //            var targetValue = GetMetricValue(
        //                logger,
        //                testRun,
        //                testContent,
        //                sparkClient,
        //                sparkClientSettings,
        //                testContent.TargetStreamPath != null ? testContent.GetCurrentTargetStreamPath() : testContent.GetCurrentStreamPath(),
        //                dataLakeClient,
        //                messageTag,
        //                cancellationToken);

        //            metricValues = new MetricValues
        //            {
        //                Baseline = targetValue,
        //                Current = metricValue
        //            };
        //            break;
        //        default:
        //            throw new NotSupportedException($"ComparisonType {testContent.ComparisonType} not supported for SparkWorker");
        //    }

        //    var results = new List<MetricsTestResult>();

        //    foreach (var threshold in testContent.Thresholds)
        //    {
        //        var result = new MetricsTestResult
        //        {
        //            ComparisonType = testContent.ComparisonType,
        //            Date = testContent.Date,
        //            BaselineMetricValue = metricValues.Baseline[threshold.Name],
        //            MetricValue = metricValues.Current[threshold.Name],
        //            LowerBoundThreshold = threshold.LowerBound,
        //            UpperBoundThreshold = threshold.UpperBound,
        //            PercentDiff = MetricValues.ComputePercentDiff(metricValues, threshold.Name),
        //            PreviousDate = testContent.GetPreviousDate(),
        //            TestName = testRun.TestName,
        //            TestRunId = testRun.Id,
        //            MetricName = threshold.Name
        //        };

        //        logger.LogEntityAsync(result, "SparkMetricsTestResult").Wait();

        //        results.Add(result);
        //    }

        //    return results;
        //}


        ///// <summary>
        ///// Gets the metric values.
        ///// </summary>
        ///// <param name="dataLakeClient">The data lake client.</param>
        ///// <param name="logger">The logger.</param>
        ///// <param name="testRun">The test run.</param>
        ///// <param name="testContent">Content of the test.</param>
        ///// <param name="sparkClient">The spark client.</param>
        ///// <param name="sparkClientSettings">The spark client settings.</param>
        ///// <param name="messageTag">The message tag.</param>
        ///// <returns>MetricValues.</returns>
        //private static MetricValues GetMetricValues(
        //    IDataLakeClient dataLakeClient,
        //    ILogger logger,
        //    TestRun testRun,
        //    SparkMetricsTestContent testContent,
        //    ISparkClient sparkClient,
        //    SparkClientSettings sparkClientSettings,
        //    SparkWorkerMessageTag messageTag,
        //    CancellationToken cancellationToken)
        //{
        //    string currentPath = testContent.GetCurrentStreamPath();
        //    string previousPath = testContent.GetPreviousStreamPath();

        //    var current = GetMetricValue(
        //        logger,
        //        testRun,
        //        testContent,
        //        sparkClient,
        //        sparkClientSettings,
        //        currentPath,
        //        dataLakeClient,
        //        messageTag,
        //        cancellationToken);
        //    var previous = GetMetricValue(
        //        logger,
        //        testRun,
        //        testContent,
        //        sparkClient,
        //        sparkClientSettings,
        //        previousPath,
        //        dataLakeClient,
        //        messageTag,
        //        cancellationToken);

        //    var metricValues = new MetricValues
        //    {
        //        Baseline = previous,
        //        Current = current
        //    };

        //    return metricValues;
        //}

        ///// <summary>
        ///// Gets the metric value.
        ///// </summary>
        ///// <param name="logger">The logger.</param>
        ///// <param name="testRun">The test run.</param>
        ///// <param name="testContent">Content of the test.</param>
        ///// <param name="sparkClient">The spark client.</param>
        ///// <param name="sparkClientSettings">The spark client settings.</param>
        ///// <param name="stream">The stream.</param>
        ///// <param name="dataLakeClient">The data lake client.</param>
        ///// <param name="messageTag">The message tag.</param>
        ///// <returns>System.Nullable&lt;System.Double&gt;.</returns>
        ///// <exception cref="InvalidOperationException">Stream does not exist : {stream}</exception>
        //private static IDictionary<string, double> GetMetricValue(
        //    ILogger logger,
        //   TestRun testRun,
        //    SparkMetricsTestContent testContent,
        //    ISparkClient sparkClient,
        //    SparkClientSettings sparkClientSettings,
        //    string stream,
        //    IDataLakeClient dataLakeClient,
        //    SparkWorkerMessageTag messageTag,
        //    CancellationToken cancellationToken)
        //{

        //    if (!dataLakeClient.CheckExists(testContent.GetDatalakeStore(), stream))
        //    {
        //        throw new FileNotFoundException($"Stream does not exist : {stream}");
        //    }

        //    var sparkRequest = new SparkClientRequest
        //    {
        //        NodeType = sparkClientSettings.NodeType,
        //        NumWorkersMin = sparkClientSettings.NumWorkersMin,
        //        NumWorkersMax = sparkClientSettings.NumWorkersMax,
        //        CostPerNode = SparkCorrectness.GetCostPerNode(sparkClientSettings.NodeTypes, sparkClientSettings.NodeType),
        //        Libraries = sparkClientSettings.Libraries,
        //        NotebookPath = testContent.NotebookPath,
        //        NotebookParameters = testContent.NotebookParameters ?? new Dictionary<string, string>(),
        //        TestRunId = testRun.Id,
        //        TimeoutSeconds = sparkClientSettings.TimeoutSeconds
        //    };

        //    logger.LogInfoAsync("002d1056-984d-4558-8ac7-60d7da619f38", $"Running notebook={testContent.NotebookPath} for DataLakeStore={testContent.GetDatalakeStore()}, Path={stream}", testRun.Id).Wait();
        //    var mountPoint = SparkCorrectness.GetMountPoint(testContent.GetDatalakeStore(), stream);
        //    logger.LogInfoAsync("f3d59ead-7890-4ced-9d78-973ff0018237", $"Running notebook={testContent.NotebookPath} for mountPoint={mountPoint}", testRun.Id).Wait();

        //    string streamPath = null;
        //    if (testContent.ConvertToParquet)
        //    {
        //        var parquetFile = messageTag.MountPointToParquetFile[mountPoint];
        //        logger.LogInfoAsync("2468acaf-fa9e-425d-9cbd-d5045038baaa", $"Running notebook={testContent.NotebookPath} using parquetFile={parquetFile}", testRun.Id).Wait();
        //        streamPath = parquetFile;
        //    }
        //    else
        //    {
        //        streamPath = mountPoint;
        //    }

        //    sparkRequest.NotebookParameters["streamPath"] = streamPath;

        //    logger.LogInfoAsync("d2970006-675b-45aa-b89d-922047c17d14", $"Notebook parameters : {string.Join(", ", sparkRequest.NotebookParameters.Select(t => t.Key + "=" + t.Value))}", testRun.Id).Wait();

        //    // Log request to OMS
        //    logger.LogEntityAsync(sparkRequest, "SparkClientRequest").Wait();

        //    var response = sparkClient.RunNotebook(sparkRequest, cancellationToken);
        //    response.TestRunId = testRun.Id;
        //    logger.LogEntityAsync(response, "SparkClientResponse").Wait();

        //    if (response.IsRunSuccess())
        //    {
        //        // For format reference see:
        //        // https://pandas.pydata.org/pandas-docs/version/0.24.2/reference/api/pandas.DataFrame.to_json.html
        //        var resultDataFrame = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, double>>>(response.RunOutput);
        //        var resultDictionary = new Dictionary<string, double>();
        //        foreach (var pair in resultDataFrame)
        //        {
        //            // pair.Value is the column name
        //            if (pair.Value == null || pair.Value.Count == 0)
        //            {
        //                throw new InvalidOperationException("Result does not contain any rows");
        //            }

        //            // We take the first row only
        //            resultDictionary.Add(pair.Key, pair.Value.First().Value);
        //        }
        //        return resultDictionary;
        //    }
        //    else
        //    {
        //        throw new SparkJobException($"Error getting metric. TestRun = {testRun.Id}, Spark job {response?.Run?.RunId} failed");
        //    }
        //}
    }
}

