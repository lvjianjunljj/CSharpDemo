We need to migrate the Kusto demo into a new project, the reason will be explained below.

Nuget lib Microsoft.Azure.Kusto.Data reference to the Nuget lib Microsoft.IdentityModel.Clients.ActiveDirectory (>= 4.5.1).We can see it in this img
![AzureKustoDataDependencies](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AzureKustoDataDependencies.png)
But in the doc from Azure/azure-sdk-for-net [Azure KeyVault Doc](https://github.com/Azure/azure-sdk-for-net/blob/62682f1faea46ffca6a7fb53c2a80dfb339cbb9d/src/SdkCommon/AppAuthentication/Azure.Services.AppAuthentication/AzureServiceTokenProvider.cs#L99), we can see that there are four ways to get secret from Azure KeyVault, and in the KeyVaultSecretProvider class in project CSharpDemo, we use the forth way. And the Azure KeyVault demo code is like this:
![AzureKeyVaultCode](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AzureKeyVaultCode.png)
We can see that there are two Nuget lib we need: Microsoft.Azure.KeyVault and Microsoft.Azure.Services.AppAuthentication. For Microsoft.Azure.KeyVault we can see that there is not conflict dependencies with Microsoft.Azure.Kusto.Data
![AzureKeyVaultDependencies](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AzureKeyVaultDependencies.png)
But for Microsoft.Azure.Services.AppAuthentication, we can see that its dependencies contanins Microsoft.IdentityModel.Clients.ActiveDirectory (>= 3.14.2) from this img
![AzureServicesAppAuthenticationDependencies](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AzureServicesAppAuthenticationDependencies.png)
Actually we will use Microsoft.IdentityModel.Clients.ActiveDirectory with version 3.14.2 by default when we install Microsoft.Azure.Services.AppAuthentication and the Azure KeyVault demo code will work.

But when we install the Microsoft.Azure.Kusto.Data, we will update the version of Microsoft.IdentityModel.Clients.ActiveDirectory to 4.5.1, then the Azure KeyVault demo code will throw a exception like this 
![AzureKeyVaultError](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AzureKeyVaultError.png)

So we need to migrate the Kusto demo into a new project and remove the reference to Microsoft.Azure.Kusto.Data and its dependencies that other libs not reference to, then update the version of Microsoft.IdentityModel.Clients.ActiveDirectory to 3.14.2(Updating the version of Newtonsoft.Json is OK for the using of Microsoft.Azure.KeyVault). But there still is this exception.

Then we found when we update the version of Microsoft.IdentityModel.Clients.ActiveDirectory to 4.5.1 to install Microsoft.Azure.Kusto.Data, we update both packages.config and App.config.
The initial App.config don't contains any version setting like this:
![AppConfigInit](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AppConfigInit.png)
And the updated App.config is that:
![AppConifgUpdate](https://github.com/lvjianjunljj/CSharpDemo/blob/master/AzureKustoDemo/info_img/AppConifgUpdate.png)
Even though finally we update the version of Microsoft.IdentityModel.Clients.ActiveDirectory to 3.14.2, but the newVersion value of Microsoft.IdentityModel.Clients.ActiveDirectory in App.config is stiil  4.5.1.0(I think it just will only be updated up but not be updated down). So we can just delete the dependentAssembly of Microsoft.IdentityModel.Clients.ActiveDirectory in App.config or update its newVersion to 3.14.2.0. Finally the Azure KeyVault demo code will work.

Note: I find there is a dependentAssembly tab for Microsoft.IdentityModel.Clients.ActiveDirectory.Platform in App.config, it also can make the Azure KeyVault demo code throw the same exception, just remove it.



