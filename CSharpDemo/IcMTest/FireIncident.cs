namespace CSharpDemo.IcMTest
{
    using Microsoft.AzureAd.Icm.Types;
    using System;
    using System.Security.Cryptography.X509Certificates;
    using System.ServiceModel;
    using System.ServiceModel.Security;

    using Microsoft.AzureAd.Icm.WebService.Client;

    class FireIncident
    {
        private static string IcMAuthCertThumbprint = "87a1331eac328ec321578c10ebc8cc4c356b005f";
        private static string IcMIncidentCreator = "DataCopMonitor";

        public static void MainMethod()
        {
            Guid targetConnector = new Guid("edaa079e-e6b7-4412-9b4d-247f48190d20");

            string title = "Test CFR Alert";
            string owningTeamId = "CUSTOMERINSIGHTANDANALYSIS\\CIA";
            string containerPublicId = "ce3e9ffa-927d-4ad3-b2bb-ad34db88c5ef";
            string routingId = "ciaprod";

            IncidentAddUpdateResult incidentAddUpdateResult = AddOrUpdateIncident(targetConnector, GenerateAlertSourceIncidentFrom(title, owningTeamId, containerPublicId, routingId));
            Console.WriteLine(incidentAddUpdateResult.IncidentId);
            Console.WriteLine(incidentAddUpdateResult.Status);
            Console.WriteLine(incidentAddUpdateResult.SubStatus);
            Console.WriteLine(incidentAddUpdateResult.UpdateProcessTime);
        }


        private static IncidentAddUpdateResult AddOrUpdateIncident(Guid targetConnector, AlertSourceIncident incident)
        {
            if (incident == null)
            {
                throw new InvalidOperationException("The incident parameter cannot be null");
            }

            try
            {
                IConnectorIncidentManager icMConnectorClient = CreateIncidentManager(IcMAuthCertThumbprint);
                return icMConnectorClient.AddOrUpdateIncident2(targetConnector, incident, RoutingOptions.None);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to send incident to IcM:\n{e.Message}");
                throw;
            }
        }

        /// <summary>
        /// For more info, see https://icmdocs.azurewebsites.net/developers/Connectors/InjectingIncidentsUsingConnectorAPI.html
        /// </summary>
        /// <param name="testRunAlert"></param>
        /// <returns></returns>
        private static AlertSourceIncident GenerateAlertSourceIncidentFrom(string title, string owningTeamId, string containerPublicId, string routingId)
        {
            DateTime now = DateTime.UtcNow;

            AlertSourceIncident alertSourceIncident = new AlertSourceIncident()
            {
                Title = title,
                Summary = "Just for test",
                Severity = 4,
                Status = IncidentStatus.Active,
                OwningTeamId = owningTeamId,
                ImpactStartDate = DateTime.Now,
                CustomFields = new IncidentConnectorCustomField[] {
                    new IncidentConnectorCustomField()
                    {
                        ContainerType = IncidentCustomFieldContainerType.Tenant,
                        ContainerPublicId = new Guid(containerPublicId),
                        FieldSet = null
                    }
                },
                Source = new AlertSourceInfo()
                {
                    CreatedBy = IcMIncidentCreator,
                    Origin = "Partner",
                    CreateDate = now,
                    ModifiedDate = now,
                    IncidentId = Guid.NewGuid().ToString("N")
                },
                OccurringLocation = new IncidentLocation
                {
                    DataCenter = "DataCop",
                    DeviceGroup = "DataCop",
                    DeviceName = "DataCopAlert",
                    Environment = "DEV",
                    ServiceInstanceId = "DataCopAlertMicroService",
                },
                RaisingLocation = new IncidentLocation
                {
                    DataCenter = "DataCop",
                    DeviceGroup = "DataCop",
                    DeviceName = "DataCopAlert",
                    Environment = "DEV",
                    ServiceInstanceId = "DataCopAlertMicroService",
                },
                RoutingId = routingId,
            };

            return alertSourceIncident;
        }



        /// <summary>
        /// Creates the specified ic m connector authentication cert thumbprint.
        /// For this fucntion, we need to add the reference to namespace System.ServiceModel.
        /// </summary>
        /// <param name="icMConnectorAuthCertThumbprint">The ic m connector authentication cert thumbprint.</param>
        /// <returns>IConnectorIncidentManager.</returns>
        public static IConnectorIncidentManager CreateIncidentManager(string icMConnectorAuthCertThumbprint)
        {
            // ppe https://icm.ad.msoppe.msft.net/Connector3/ConnectorIncidentManager.svc
            string icmAlertEndpoint = "https://icm.ad.msft.net/Connector3/ConnectorIncidentManager.svc";

            ConnectorIncidentManagerClient client;
            WS2007HttpBinding binding;
            EndpointAddress remoteAddress;

            binding = new WS2007HttpBinding(SecurityMode.Transport)
            {
                Name = "IcmBindingConfigCert",
                MaxBufferPoolSize = 4194304,
                MaxReceivedMessageSize = 16777216,
                SendTimeout = TimeSpan.FromMinutes(2)
            };

            binding.Security.Transport.Realm = string.Empty;
            binding.Security.Transport.ProxyCredentialType = HttpProxyCredentialType.None;
            binding.Security.Transport.ClientCredentialType = HttpClientCredentialType.Certificate;

            binding.ReaderQuotas.MaxArrayLength = 16384;
            binding.ReaderQuotas.MaxBytesPerRead = 1048576;
            binding.ReaderQuotas.MaxStringContentLength = 1048576;

            binding.Security.Message.EstablishSecurityContext = false;
            binding.Security.Message.NegotiateServiceCredential = true;
            binding.Security.Message.AlgorithmSuite = SecurityAlgorithmSuite.Default;
            binding.Security.Message.ClientCredentialType = MessageCredentialType.Certificate;

            remoteAddress = new EndpointAddress(icmAlertEndpoint);
            client = new ConnectorIncidentManagerClient(binding, remoteAddress);

            if (client.ClientCredentials != null)
            {
                client.ClientCredentials.ClientCertificate.SetCertificate(
                        StoreLocation.CurrentUser, StoreName.My, X509FindType.FindByThumbprint, icMConnectorAuthCertThumbprint);
            }

            return client;
        }
    }
}
