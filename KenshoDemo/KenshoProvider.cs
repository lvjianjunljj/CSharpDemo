namespace KenshoDemo
{
    using KenshoDemo.Models;
    using System;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// This interface defines all the calls to Kensho we've implemented.
    /// </summary>
    public sealed class KenshoProvider
    {
        private const string AnomalyDetectorTimeSeriesLastDetect = "anomalydetector/v1.0/timeseries/last/detect";

        private static readonly HttpClient httpClient = new HttpClient();
        private readonly KenshoSettings kenshoSettings;

        public KenshoProvider(KenshoSettings kenshoSettings)
        {
            this.kenshoSettings = kenshoSettings;
        }

        /// <summary>
        /// This returns the configuration settings related to Kensho.
        /// </summary>
        /// <returns>KenshoSettings</returns>
        public KenshoSettings GetKenshoSettings()
        {
            return this.kenshoSettings;
        }

        /// <summary>
        /// Just a generic message and get the response.
        /// </summary>
        /// <param name="json">The json for the request.</param>
        /// <param name="apiCommand">The api command that Kensho recognizes.</param>
        /// <returns></returns>
        public async Task<KenshoBaseResponse> SendRequestAsync(string json, string apiCommand)
        {
            // Most requests go to Kensho standard API, but one goes to the backend API (this is the Azure cogitive API for the 
            // Kensho connector).  Documentation on this can be found at:
            //   https://westus2.dev.cognitive.microsoft.com/docs/services/AnomalyDetector/operations/post-timeseries-last-detect
            bool useBackend = (apiCommand == AnomalyDetectorTimeSeriesLastDetect);

            string url = useBackend ?
                $"{this.kenshoSettings.BackendAPIURL}/{apiCommand}" :
                $"{this.kenshoSettings.APIURL}/{apiCommand}";
            KenshoBaseResponse response;

            try
            {
                HttpRequestMessage requestMessage = new HttpRequestMessage(HttpMethod.Post, new Uri(url));
                requestMessage.Content = new StringContent(json, Encoding.UTF8);
                requestMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                requestMessage.Headers.Add("Accept", "application/json");
                if (useBackend)
                {
                    requestMessage.Headers.Add("Ocp-Apim-Subscription-Key", this.kenshoSettings.BackendAPITenantKey);
                }
                else
                {
                    requestMessage.Headers.Add("x-api-key", this.kenshoSettings.APITenantKey);
                }
                HttpResponseMessage httpResponse = await httpClient.SendAsync(requestMessage);
                response = new KenshoBaseResponse()
                {
                    StatusCode = httpResponse.StatusCode,
                    ContentOrErrorMessage = httpResponse.Content != null ? await httpResponse.Content.ReadAsStringAsync() : "",
                };
            }
            catch (Exception excep)
            {
                Console.WriteLine($"Kensho API Post Exception: {excep.ToString()}");
                response = new KenshoBaseResponse()
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    ContentOrErrorMessage = $"Kensho API Post Exception: {excep.Message}",
                };
            }

            return response;
        }
    }
}

