namespace AzureSparkDemo.Clients
{
    using Microsoft.Azure.Databricks.Client;

    public class DatabricksClientWrapper
    {
        /// <summary>
        /// The base URL
        /// </summary>
        private string baseUrl;
        /// <summary>
        /// The token
        /// </summary>
        private string token;

        public DatabricksClientWrapper(string baseUrl, string token)
        {
            this.baseUrl = baseUrl;
            this.token = token;
        }

        /// <summary>
        /// Deletes the DBFS path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="recurse">if set to <c>true</c> [recurse].</param>
        public void DbfsDelete(string path, bool recurse)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                client.Dbfs.Delete(path, recurse);
            }
        }

        public long JobsRunSubmit(RunOnceSettings settings)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                return client.Jobs.RunSubmit(settings).Result;
            }
        }

        /// <summary>
        /// Gets the runs for job.
        /// </summary>
        /// <param name="runId">The run identifier.</param>
        /// <returns>Run.</returns>
        public Run JobsRunsGet(long runId)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                return client.Jobs.RunsGet(runId).Result;
            }
        }

        /// <summary>
        /// Gets the output for the job.
        /// </summary>
        /// <param name="runId">The run identifier.</param>
        /// <returns>System.ValueTuple&lt;System.String, System.String, Run&gt;.</returns>
        public (string, string, Run) JobsRunsGetOutput(long runId)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                return client.Jobs.RunsGetOutput(runId).Result;
            }
        }

        /// <summary>
        /// Runs the job now.
        /// </summary>
        /// <param name="jobId">The job identifier.</param>
        /// <param name="runParameters">The run parameters.</param>
        /// <returns>RunIdentifier.</returns>
        public RunIdentifier JobsRunNow(long jobId, RunParameters runParameters)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                return client.Jobs.RunNow(jobId, runParameters).Result;
            }
        }

        /// <summary>
        /// Imports the workspace.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="format">The format.</param>
        /// <param name="language">The language.</param>
        /// <param name="content">The content.</param>
        /// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        public void WorkspaceImport(string path, ExportFormat format, Language? language, byte[] content, bool overwrite)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                client.Workspace.Import(path, format, language, content, overwrite).Wait();
            }
        }

        /// <summary>
        /// Deletes jobs.
        /// </summary>
        /// <param name="jobId">The job identifier.</param>
        public void JobsDelete(long jobId)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                client.Jobs.Delete(jobId).Wait();
            }
        }

        public void JobsRunCancel(long runId)
        {
            using (var client = DatabricksClient.CreateClient(this.baseUrl, this.token))
            {
                client.Jobs.RunsCancel(runId).Wait();
            }
        }
    }
}
