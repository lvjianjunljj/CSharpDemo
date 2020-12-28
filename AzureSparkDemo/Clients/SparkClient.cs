namespace AzureSparkDemo.Clients
{
    using AzureSparkDemo.Models;
    using CommonLib.IDEAs;
    using Microsoft.Azure.Databricks.Client;
    using System;
    using System.Threading;

    public class SparkClient
    {
        DatabricksClientWrapper client;

        /// <summary>
        /// Initializes a new instance of the <see cref="SparkClient" /> class.
        /// </summary>
        /// <param name="baseUrl">The base URL.</param>
        /// <param name="token">The token.</param>
        public SparkClient(DatabricksClientWrapper client)
        {
            this.client = client;
        }

        /// <summary>
        /// Deletes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="recurse">if set to <c>true</c> [recurse].</param>
        public void Delete(string path, bool recurse)
        {
            this.client.DbfsDelete(path, recurse);
        }

        public SparkClientResponse RunNotebook(SparkClientRequest request, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return new SparkClientResponse
                {
                    Run = new Run
                    {
                        State = new RunState { ResultState = RunResultState.CANCELED }
                    }
                };
            }


            // We must have a timeout.
            if (request.TimeoutSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException("TimeoutSeconds");
            }

            var runStartTime = DateTime.UtcNow;
            Console.WriteLine($"[{runStartTime.ToString("o")}] SparkClient.RunNotebook() started.");
            Console.WriteLine($"Using TimeoutSeconds={request.TimeoutSeconds}");
            var notebookPath = request.NotebookPath;
            // New cluster config
            var newCluster = GetDefaultClusterInfo(request.NumWorkersMin, request.NumWorkersMax, request.NodeType);

            Console.WriteLine($"SparkClient: Creating new cluster with NumWorkers=({newCluster.AutoScale.MinWorkers},{newCluster.AutoScale.MaxWorkers}), NodeType={newCluster.NodeTypeId}, Runtime={newCluster.RuntimeVersion}");

            var runOnceSettings = new RunOnceSettings
            {
                RunName = notebookPath + "Job",
                Libraries = request.Libraries,
                NewCluster = newCluster,
                NotebookTask = new NotebookTask
                {
                    BaseParameters = request.NotebookParameters,
                    NotebookPath = notebookPath
                },
                TimeoutSeconds = request.TimeoutSeconds
            };

            // Start the job and retrieve the run id.
            SparkClientResponse response = null;

            // Create new job
            var runId = Retrier.Retry(() => client.JobsRunSubmit(runOnceSettings));
            Run run = null;

            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    client.JobsRunCancel(runId);

                    return new SparkClientResponse
                    {
                        Run = new Run
                        {
                            State = new RunState { ResultState = RunResultState.CANCELED }
                        }
                    };
                }

                // Keep polling the run by calling RunsGet until run terminates:
                run = Retrier.Retry<Run>(() => client.JobsRunsGet(runId));

                Console.WriteLine($"SparkClient: RunId = {runId} returned status {run.State.StateMessage}");

                if (run.State.ResultState.HasValue)
                {
                    break;
                }
                Thread.Sleep(60 * 1000);
            }

            response = new SparkClientResponse
            {
                Run = run,
                RunOutput = null,
                TestRunId = request.TestRunId
            };

            string runOutputText = null;
            if (response.IsRunSuccess())
            {
                var runOutput = Retrier.Retry(() => client.JobsRunsGetOutput(run.RunId));
                runOutputText = runOutput.Item1;
                response.RunOutput = runOutputText;
            }

            var runEndTime = DateTime.UtcNow;
            var totalElapsed = runEndTime - runStartTime;

            // Calculate cost
            {
                response.NumWorkersMin = request.NumWorkersMin;
                response.NumWorkersMax = request.NumWorkersMax;
                response.CostPerNode = request.CostPerNode;
                response.TotalHours = totalElapsed.TotalHours;
                response.NodeType = request.NodeType;
                // The plus one is the driver node. 
                response.Cost = response.CostPerNode * response.TotalHours * (Average(request.NumWorkersMin, request.NumWorkersMax) + 1);
            }

            Console.WriteLine($"SparkClient: RunId = {runId}: Ended. Result:{run?.State?.ResultState}. Result from Notebook : {runOutputText}");
            Console.WriteLine($"[{runEndTime.ToString("o")}] RunId={runId}. Completed SparkClient.RunNotebook(), Elapsed Time = {totalElapsed}");
            Console.WriteLine($"[{runEndTime.ToString("o")}] RunId={runId}. NumWorkers=({response.NumWorkersMin},{response.NumWorkersMax}), CostPerNode={response.CostPerNode}, TotalHours={response.TotalHours}, Cost=${response.Cost}");

            return response;
        }

        public static double Average(double a, double b)
        {
            return (a + b) / 2.0;
        }

        /// <summary>
        /// Gets the default cluster information.
        /// </summary>
        /// <param name="workers">The workers.</param>
        /// <param name="nodeType">Type of the node.</param>
        /// <returns>ClusterInfo.</returns>
        private static ClusterInfo GetDefaultClusterInfo(
            int workersMin,
            int workersMax,
            string nodeType)
        {
            var newCluster = ClusterInfo.GetNewClusterConfiguration()
                .WithAutoScale(minWorkers: workersMin, maxWorkers: workersMax)
                .WithPython3(true)
                .WithNodeType(nodeType)
                .WithRuntimeVersion(RuntimeVersions.Runtime_5_5);
            return newCluster;
        }
    }
}
