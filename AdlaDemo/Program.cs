namespace AdlaDemo
{
    using Microsoft.Azure.Management.DataLake.Analytics;
    using Microsoft.Azure.Management.DataLake.Analytics.Models;
    using Microsoft.Azure.Management.DataLake.InternalAnalytics.Scope;
    using Microsoft.Azure.Management.DataLake.Store;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;


    class Program
    {
        static string _tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        static string _clientId = "86797b72-1a07-4c52-89a6-2cb3320f5cd8";
        static string _clientSecret = "";
        static string _clientCertificateThumbprint = "28c0141ee5bfae0bcda388f5d76870d5bc2d6010";

        static string _adlaAccountName = "sandbox-c08";
        static string _adlsAccountName = "sandbox-c08";
        static string _accountSuffix = "azuredatalakestore.net";
        //static string _userName = Environment.UserName;
        static string _userName = "jianjlv_test";

        static string _ClusterInputFilePath = string.Format("/local/temp/{0}/origin.tsv", _userName);
        static string _ClusterReferenceFilePath = string.Format("/local/temp/{0}/cluster-reference.txt", _userName);

        private static string _ScopeScriptName
        {
            get { return String.Format("SubmitScope example ({0})", _userName); }
        }

        static void Main(string[] args)
        {
            //_clientId = "a3bdd828-cb26-41e7-8437-e5d73ced7ab9";
            //_clientSecret = "";

            //_clientId = "263cd72a-ed4f-4deb-be71-24762bf7b6d2";
            //_clientSecret = "";

            //_clientId = "86797b72-1a07-4c52-89a6-2cb3320f5cd8";
            //_clientSecret = "";

            //_clientId = "94439138-c707-45e1-832e-0ddbd85ec671";
            //_clientSecret = "";

            //_adlaAccountName = "ideas-ppe-c14";
            //_adlsAccountName = "ideas-ppe-c14";

            //_adlaAccountName = "sandbox-c08";
            //_adlsAccountName = "sandbox-c08";


            // Authenticate
            var authContext = new AuthenticationContext(string.Format("https://login.windows.net/{0}", _tenantId));

            // swap these lines to switch between certificate and secret authentication
            var clientCredentials = new ClientCredential(_clientId, _clientSecret);
            //var clientCredentials = new ClientAssertionCertificate(_clientId, Utility.GetCertificateFromStore(_clientCertificateThumbprint, ""));

            var authResult = authContext.AcquireTokenAsync("https://management.core.windows.net/", clientCredentials).GetAwaiter().GetResult();

            if (authResult == null)
                throw new InvalidOperationException("Failed to obtain the JWT token");

            // Get access token based upon access request.
            var tokenCred = new TokenCredentials(authResult.AccessToken);

            var internalJobClient = new DataLakeInternalAnalyticsScopeJobManagementClient(tokenCred);
            var publicJobClient = new DataLakeAnalyticsJobManagementClient(tokenCred);
            var publicStoreClient = new DataLakeStoreFileSystemManagementClient(tokenCred);

            string localInputFile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), "files\\origin.tsv");
            string localClusterReferenceFile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), "files\\cluster-reference.txt");

            publicStoreClient.FileSystem.UploadFile(_adlsAccountName, sourcePath: localInputFile, destinationPath: _ClusterInputFilePath, overwrite: true);
            publicStoreClient.FileSystem.UploadFile(_adlsAccountName, sourcePath: localClusterReferenceFile, destinationPath: _ClusterReferenceFilePath, overwrite: true);

            // Set External Parameters
            Dictionary<string, string> extParameters = new Dictionary<string, string>();
            extParameters.Add("UserName", string.Format("\"{0}\"", _userName));
            extParameters.Add("Parameter1", "\"ParameterOne\"");

            // Job Submission
            string scriptFile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), "files\\origin-resources.script");
            string scriptText = File.ReadAllText(scriptFile);
            string dataRoot = string.Format("adl://{0}.{1}/", _adlaAccountName, _accountSuffix);
            string scriptUpdatedFile = string.Empty;
            ScopeCompiler.Parser.ParseInformation parserInfo = ScopeClient.Scope.ExtractJobResources(scriptFile, null, dataRoot, authResult.AccessToken, extParameters, " -on adl ", null, out scriptUpdatedFile);
            scriptText = File.ReadAllText(scriptUpdatedFile);

            // Local Compile
            //ScopeClient.Scope.Compile(dataRoot: dataRoot, accessToken: authResult.AccessToken,
            //scopeScriptFilePath: scriptUpdatedFile, parameters: extParameters, compilerOptions: null);

            List<ScopeJobResource> localRes = parserInfo.GetResources(true).Select(f => new ScopeJobResource(Path.GetFileNameWithoutExtension(f), f)).ToList();
            List<ScopeJobResource> clusterRes = parserInfo.GetResources(false).Select(f => new ScopeJobResource(Path.GetFileNameWithoutExtension(f), f)).ToList();

            // Submit job
            // Includes the recurrence name so that this job can be analyzed in Job Insights in the Azure Portal
            Guid relationGuid = Guid.Parse(_tenantId);
            var scopeProperties = new CreateScopeJobProperties(scriptText, resources: clusterRes);
            var scopeParameters = new CreateScopeJobParameters(JobType.Scope, scopeProperties, _ScopeScriptName, priority: 900, related: new JobRelationshipProperties(relationGuid, recurrenceName: "Submit Scope Example Recurring Job"));

            var jobId = Guid.NewGuid();
            JobInformation jobInfo = internalJobClient.Extension.Create(_adlaAccountName, jobId, scopeParameters,
                new DataLakeStoreAccountInformation(name: _adlaAccountName, suffix: _accountSuffix), localRes);
            //JobInformation jobInfo = publicJobClient.Job.Create(_adlaAccountName, jobId, scopeParameters);

            Console.WriteLine("{0} Submitted Job Id:{1}", DateTime.Now, jobInfo.JobId);

            // Use the public job client to get status
            if (publicJobClient.Job.Exists(_adlaAccountName, jobInfo.JobId.Value))
            {
                // Get Job Information
                JobInformation job = publicJobClient.Job.Get(_adlaAccountName, jobInfo.JobId.Value);

                // Wait for Job to Finish, with timeout
                var timeout = DateTime.UtcNow.AddMinutes(20);
                Console.WriteLine("{0} Waiting for job to finish", DateTime.Now);

                while (job.Result == JobResult.None && DateTime.UtcNow < timeout)
                {
                    job = publicJobClient.Job.Get(_adlaAccountName, jobInfo.JobId.Value);
                    Console.WriteLine("{0} - {1}", DateTime.Now, job.State);
                    System.Threading.Thread.Sleep(5 * 1000);
                }

                // Cancel if over X minutes
                if (DateTime.UtcNow >= timeout)
                    publicJobClient.Job.Cancel(_adlaAccountName, jobInfo.JobId.Value);

                Console.WriteLine("{0} Job Result: {1}", DateTime.Now, job.Result);
            }

            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit");
                Console.ReadLine();
            }
        }
    }
}
