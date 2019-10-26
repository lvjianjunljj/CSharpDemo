namespace MetagraphDemo
{
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    class PostOperation
    {
        public static void MainMethod()
        {

            //JObject interfaceJObejct = GenerateInterfaceJObejctSample(out string interfaceId);
            JObject interfaceJObejct = GetOperation.GetIDEAsDataInterface("a75b373c-0b21-428c-a44e-207a2883872c");
            string interfaceId = Guid.NewGuid().ToString();

            interfaceJObejct["identifier"] = interfaceId;
            interfaceJObejct["name"] = "test_jianjlv1111";

            PostIDEAsDataInterface(interfaceId, interfaceJObejct.ToString());
            Console.WriteLine($"interface id: {interfaceId}");
            GetOperation.GetIDEAsDataInterface(interfaceId);

            //JObject onBoardRequestJObejct = GenerateOnBoardRequestJObejctSample(out string onBoardRequestId);

            //PostIDEAsOnBoardRequest(onBoardRequestId, onBoardRequestJObejct.ToString());
            //Console.WriteLine($"onBoardRequest id: {onBoardRequestId}");

            //GetOperation.GetIDEAsOnBoardRequest("40dc20a6-3769-4271-816b-9172d7d09241");

        }

        private static string metagraphRootUrl = Constant.METAGRAPH_ROOT_URL;

        // This is the tenant id for Microsoft
        private static string microsoftTenantId = Constant.MICROSOFT_TENANT_ID;
        private static string client_id = Constant.CLINT_ID;
        private static string client_secret = Constant.CLINT_SECRET;
        private static string resource = Constant.METAGRAPH_RESOURCE;


        private static void PostIDEAsDataInterface(string id, string interfaceContent)
        {
            Console.WriteLine($"Post IDEAsDataInterface {id}");
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})";

            string respString = SendRequestString(url, "Post", interfaceContent).Result;
            Console.WriteLine(respString);
        }


        private static void PostIDEAsOnBoardRequest(string id, string onBoardRequestContent)
        {
            Console.WriteLine($"Post IDEAsOnBoardRequest {id}");
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests";

            string respString = SendRequestString(url, "Post", onBoardRequestContent).Result;
            Console.WriteLine(respString);
        }


        private static async Task<string> SendRequestString(string url, string requestMethod, string reqeustContent = null)
        {
            HttpClient httpClient = new HttpClient();
            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod(requestMethod), url);

            if (!string.IsNullOrEmpty(reqeustContent))
            {
                request.Content = new StringContent(reqeustContent,
                                   Encoding.UTF8,
                                   "application/json");
            }

            var token = GetToken();
            request.Headers.Add("Authorization", $"Bearer {token}");
            using (HttpResponseMessage response = httpClient.SendAsync(request).Result)
            {
                string respString = await response.Content.ReadAsStringAsync();
                return respString;
            }
        }

        private static string GetToken()
        {
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{microsoftTenantId}", TokenCache.DefaultShared);

            ClientCredential clientCred = new ClientCredential(client_id, client_secret);
            // Function AcquireTokenAsync() has multiple overloads
            var authenticationResult = authenticationContext.AcquireTokenAsync(resource, clientCred).Result;
            return authenticationResult.AccessToken;
        }

        private static JObject GenerateInterfaceJObejctSample(out string interfaceId)
        {
            interfaceId = Guid.NewGuid().ToString();
            JObject interfaceJObejct = new JObject();
            interfaceJObejct["@odata.context"] = "https://api.metagraph.officeppe.net/2.0/$metadata#IDEAsDataInterfaces/$entity";
            interfaceJObejct["createdByDisplayName"] = "Metagraph Logic App Client";
            interfaceJObejct["modifiedByDisplayName"] = "Metagraph Logic App Client";
            interfaceJObejct["createdByUrl"] = "https://metagraph.office.net/";
            interfaceJObejct["modifiedByUrl"] = "https://metagraph.office.net/";
            interfaceJObejct["childLabels"] = "IDEAsDataInterface";
            interfaceJObejct["completenessScore"] = 0.0;
            interfaceJObejct["usageScore"] = 0.0;
            interfaceJObejct["centralityScore"] = 0.0;
            interfaceJObejct["identifier"] = interfaceId;
            interfaceJObejct["name"] = "test_jianjlv1111";
            interfaceJObejct["createdDateTime"] = "2019-07-09T19:27:24.2347637Z";
            interfaceJObejct["modifiedDateTime"] = "2019-10-01T23:41:53.5350009Z";
            interfaceJObejct["createdBy"] = "ce518ae5-a4c1-446a-ac84-754351ec5230";
            interfaceJObejct["modifiedBy"] = "ce518ae5-a4c1-446a-ac84-754351ec5230";
            interfaceJObejct["ttl"] = 0;
            interfaceJObejct["state"] = "NotDefined";
            interfaceJObejct["whoAreThey"] = "Experiences & Devices > M365 Core > O365 Substrate > IDEAS : IDEAs";
            interfaceJObejct["description"] = "test00001";
            interfaceJObejct["interfaceId"] = "3e1cb137-3df2-49a6-907f-b42a87d15958";
            interfaceJObejct["documentation"] = "asd";
            interfaceJObejct["icmConnectorId"] = "64c4853b-78cd-41ff-8eff-65083863a2e1";
            interfaceJObejct["icmTenantTeamId"] = "47553";
            interfaceJObejct["icmTenantTeamPublicId"] = "IDEAS\\Triage";
            interfaceJObejct["icmServiceTreeServiceId"] = "7d8d33ce-be81-49ae-8148-c39abff531f7";
            interfaceJObejct["onboardingType"] = "Manual";
            interfaceJObejct["onboardingSourceId"] = "";
            interfaceJObejct["sourceTags"] = new JArray();
            interfaceJObejct["privacyCategoryTags"] = new JArray();
            interfaceJObejct["businessCategoryTags"] = new JArray();
            interfaceJObejct["source"] = "Azure";
            interfaceJObejct["privacyCategory"] = "AccessControlData";
            interfaceJObejct["businessCategory"] = "Commercial";
            interfaceJObejct["category"] = "Public";
            interfaceJObejct["dataFabric"] = "CosmosStream";
            interfaceJObejct["dataType"] = "Other";
            interfaceJObejct["connectionInfo"] = "{\n   \"vcName\": \"test00001\",\n   \"path\": \"test00001\",\n   \"adls\": \"test00001\"\n}";
            interfaceJObejct["sla"] = "4";
            interfaceJObejct["grain"] = "Daily";
            interfaceJObejct["rollingWindow"] = null;
            interfaceJObejct["startDate"] = "2019-04-14T15:00:00Z";
            interfaceJObejct["endDate"] = "2019-04-29T03:45:00Z";
            interfaceJObejct["parameters"] = null;
            interfaceJObejct["onboardRequestCreatedBy"] = "haridura@microsoft.com";
            interfaceJObejct["onboardRequestCreatedByDisplayName"] = "Hari Durairaj";
            interfaceJObejct["onboardRequestCreatedByURL"] = "https://teams.microsoft.com/_#/onversations/8:orgid:827b67e4-9ed7-47ff-ad2e-b1ade030f79e?ctx=chat";
            return interfaceJObejct;
        }
        private static JObject GenerateOnBoardRequestJObejctSample(out string onBoardRequestId)
        {
            onBoardRequestId = Guid.NewGuid().ToString();
            JObject onBoardRequestJObejct = new JObject();

            onBoardRequestJObejct["@odata.context"] = "https://api.metagraph.officeppe.net/2.0/$metadata#IDEAsDataInterfaces/$entity";
            onBoardRequestJObejct["createdByDisplayName"] = "Jianjun Lv";
            onBoardRequestJObejct["modifiedByDisplayName"] = "Jianjun Lv";
            onBoardRequestJObejct["createdByUrl"] = "https://metagraph.office.net/";
            onBoardRequestJObejct["modifiedByUrl"] = "https://metagraph.office.net/";
            onBoardRequestJObejct["childLabels"] = "IDEAsOnboardRequest";
            onBoardRequestJObejct["completenessScore"] = 0.0;
            onBoardRequestJObejct["usageScore"] = 0.0;
            onBoardRequestJObejct["centralityScore"] = 0.0;
            onBoardRequestJObejct["identifier"] = onBoardRequestId;
            onBoardRequestJObejct["name"] = "test_jianjlv1111";
            onBoardRequestJObejct["createdDateTime"] = "2019-07-09T19:27:24.2347637Z";
            onBoardRequestJObejct["modifiedDateTime"] = "2019-10-01T23:41:53.5350009Z";
            onBoardRequestJObejct["createdBy"] = "ce518ae5-a4c1-446a-ac84-754351ec5230";
            onBoardRequestJObejct["modifiedBy"] = "ce518ae5-a4c1-446a-ac84-754351ec5230";
            onBoardRequestJObejct["ttl"] = 0;
            onBoardRequestJObejct["whoAreThey"] = "Experiences & Devices > M365 Core > O365 Substrate > IDEAS : IDEAs";
            onBoardRequestJObejct["description"] = "test00001";
            onBoardRequestJObejct["interfaceId"] = "3e1cb137-3df2-49a6-907f-b42a87d15958";
            onBoardRequestJObejct["documentation"] = "asd";
            onBoardRequestJObejct["icmConnectorId"] = "64c4853b-78cd-41ff-8eff-65083863a2e1";
            onBoardRequestJObejct["icmTenantTeamId"] = "47553";
            onBoardRequestJObejct["icmTenantTeamPublicId"] = "IDEAS\\Triage";
            onBoardRequestJObejct["icmServiceTreeServiceId"] = "7d8d33ce-be81-49ae-8148-c39abff531f7";
            onBoardRequestJObejct["onboardingType"] = "Manual";
            onBoardRequestJObejct["onboardingSourceId"] = "";
            onBoardRequestJObejct["sourceTags"] = new JArray();
            onBoardRequestJObejct["privacyCategoryTags"] = new JArray();
            onBoardRequestJObejct["businessCategoryTags"] = new JArray();
            onBoardRequestJObejct["source"] = "Azure";
            onBoardRequestJObejct["privacyCategory"] = "AccessControlData";
            onBoardRequestJObejct["businessCategory"] = "Commercial";
            onBoardRequestJObejct["category"] = "Public";
            onBoardRequestJObejct["dataFabric"] = "CosmosStream";
            onBoardRequestJObejct["dataType"] = "Other";
            onBoardRequestJObejct["connectionInfo"] = "{\n   \"vcName\": \"test00001\",\n   \"path\": \"test00001\",\n   \"adls\": \"test00001\"\n}";
            onBoardRequestJObejct["sla"] = "4";
            onBoardRequestJObejct["grain"] = "Daily";
            onBoardRequestJObejct["rollingWindow"] = null;
            onBoardRequestJObejct["startDate"] = "2019-04-14T15:00:00Z";
            onBoardRequestJObejct["endDate"] = "2019-04-29T03:45:00Z";
            onBoardRequestJObejct["parameters"] = null;
            onBoardRequestJObejct["onboardRequestCreatedBy"] = "haridura@microsoft.com";
            onBoardRequestJObejct["onboardRequestCreatedByDisplayName"] = "Hari Durairaj";
            onBoardRequestJObejct["onboardRequestCreatedByURL"] = "https://teams.microsoft.com/_#/onversations/8:orgid:827b67e4-9ed7-47ff-ad2e-b1ade030f79e?ctx=chat";
            return onBoardRequestJObejct;
        }
    }
}