namespace CSharpDemo
{
    using AzureLib.KeyVault;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    public class QPSTestDemo
    {
        public static void MainMethod()
        {
            TestCloudScopeQPS();
        }

        private static void TestCloudScopeQPS()
        {
            string token = GetGDPRCloudScopeTokenByAADLib();

            //Console.WriteLine("joblist");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/jobList",
            //    @"Get",
            //    token,
            //    string.Empty);

            //Console.WriteLine("jobInfo");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/jobInfo/fab038d1-4fbf-492e-80cf-b7476eea7494",
            //    @"Get",
            //    token,
            //    string.Empty);

            //Console.WriteLine("jobSubmissionStatus");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/jobSubmissionStatus/fab038d1-4fbf-492e-80cf-b7476eea7494",
            //    @"Get",
            //    token,
            //    string.Empty);

            //Console.WriteLine("jobAnalysis");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/jobAnalysis?jobId=2db4018c-16b6-4ec9-ad1d-43dff0172214",
            //    @"Get",
            //    token,
            //    string.Empty);

            //Console.WriteLine("jobcancel");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/canceljob/1983d959-1963-4536-9236-845d9b88a31b",
            //    @"Put",
            //    token,
            //    string.Empty);

            //Console.WriteLine("submitJob");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/submitJob?workloadqueuename=gdpr",
            //    @"Post",
            //    token,
            //    File.ReadAllText(@"D:\data\company_work\M365\IDEAs\cloudscope\submitjobcontent.json"));

            //Console.WriteLine("externaltracker");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/externaltracker",
            //    @"Post",
            //    token,
            //    File.ReadAllText(@"D:\data\company_work\M365\IDEAs\cloudscope\externaltrackercontent.json"));

            //Console.WriteLine("deletestream");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/stream?streamRelativePath=%2Flocal%2Fusers%2Fxiaress%2FWriteStreamTest.txt",
            //    @"Delete",
            //    token,
            //    string.Empty);

            Console.WriteLine("writestream");
            RunTestByMultiThread(60, 10, new HttpClient(),
                @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/stream?streamRelativePath=%2Flocal%2Fusers%2Fxiare%2FWriteStreamTest.txt&append=false&streamExpiryInDays=20",
                @"Post",
                token,
                "\"Test for write stream via adls\"");

            //Console.WriteLine("readstream");
            //RunTestByMultiThread(60, 10, new HttpClient(),
            //    @"http://cloudscope-ppe-gdpr.asecloudscopeppe.p.azurewebsites.net/api/virtualCluster/ideas-prod-c14/stream?streamRelativePath=%2Flocal%2Fusers%2Fxiaress%2FWriteStreamTest.txt",
            //    @"Get",
            //    token,
            //    string.Empty);
        }

        private static string GetGDPRCloudScopeTokenByAADLib()
        {
            string tenantId = @"cdc5aeea-15c5-4db6-b079-fcadd2505dc2";
            string client_id = @"8e2888f4-cebf-47ad-9811-53a584413523";
            string client_secret = AzureKeyVaultDemo.GetSecret("cloudscopeppe", "GdprAadAuthAppSecret");
            string resource = @"api://ead06413-cb7c-408e-a533-2cdbe58bf3a6";
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{tenantId}", TokenCache.DefaultShared);

            ClientCredential clientCred = new ClientCredential(client_id, client_secret);
            var authenticationResult = authenticationContext.AcquireTokenAsync(resource, clientCred).Result;

            return authenticationResult.AccessToken;
        }

        private static void RunTestByMultiThread(int threadCount, int seconds, HttpClient client, string url, string requestMethod, string token, string reqeustContent = null)
        {
            QPSTestDemo demo = new QPSTestDemo();
            var runTasks = new Task<int>[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                runTasks[i] = Task.Run(async () => await demo.GetRequestCount(seconds, client, url, requestMethod, token, reqeustContent));
            }
            Task.WaitAll(runTasks);

            int sum = 0;
            for (int i = 0; i < threadCount; i++)
            {
                sum += runTasks[i].Result;
            }
            Console.WriteLine($"Request sum: {sum}");
        }


        private async Task<int> GetRequestCount(int seconds, HttpClient client, string url, string requestMethod, string token, string reqeustContent = null)
        {
            int count = 0;
            DateTime endTime = DateTime.UtcNow.AddSeconds(seconds);
            while (DateTime.UtcNow < endTime)
            {
                HttpRequestMessage request = this.GenerateRequest(url, requestMethod, token, reqeustContent);
                if (await this.SendRequestSuccessfully(client, request))
                {
                    count++;
                }
            }
            return count;
        }

        private HttpRequestMessage GenerateRequest(string url, string requestMethod, string token, string reqeustContent = null)
        {
            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod(requestMethod), url);

            if (!string.IsNullOrEmpty(reqeustContent))
            {
                request.Content = new StringContent(reqeustContent,
                                   Encoding.UTF8,
                                   "application/json");
            }

            request.Headers.Add("Authorization", $"Bearer {token}");
            return request;
        }

        private async Task<bool> SendRequestSuccessfully(HttpClient client, HttpRequestMessage request)
        {
            using (HttpResponseMessage response = await client.SendAsync(request))
            {
                //string respString = await response.Content.ReadAsStringAsync();
                //if (respString.Length > 10000)
                //{
                //    Console.WriteLine(response.StatusCode);
                //}
                //else
                //{
                //    Console.WriteLine(respString);
                //}
                var succeed = response.StatusCode == HttpStatusCode.OK || response.StatusCode == HttpStatusCode.Accepted;
                    Console.WriteLine(response.StatusCode);
                if (!succeed)
                {
                    string respString = await response.Content.ReadAsStringAsync();
                    Console.WriteLine(respString);
                }
                return true;
            }
        }






        // Demo Code for running by multi threads
        private static void RunTestByMultiThread(int threadCount)
        {
            QPSTestDemo demo = new QPSTestDemo();
            var runTasks = new Task[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                runTasks[i] = Task.Run(async () => await demo.RunTest());
            }
            Task.WaitAll(runTasks);
        }

        private async Task RunTest()
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine(DateTime.UtcNow);
                await Task.Delay(1000);
            }
        }


    }
}
