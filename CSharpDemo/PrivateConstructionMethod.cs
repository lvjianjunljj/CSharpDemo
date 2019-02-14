using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Threading.Tasks;
using CSharpDemoAux;
namespace CSharpDemo
{
    class ConfigurationManagerAppSettings
    {
        //Get the setting value from the "<appSettings>" part in the config file "app.config".

        /* 
         * Function ConfigurationManager.AppSettings.Get will get the value from the file "app.config" in main method running project,
         * not currenting project, so for here, the configurationContent1 and configurationContent2 are the same as configurationContent0.
         * It is the setting value for "AppKey" from the file "app.config" in project CSharpDemo not CSharpDemoAux.
         */
        public static void MainMethod()
        {
            Console.WriteLine(0);
            string configurationContent0 = ConfigurationManager.AppSettings.Get("AppKey");
            Console.WriteLine(configurationContent0);

            Console.WriteLine(1);
            string configurationContent1 = ShowConfigurationContentClass.GetConfigurationContent();
            Console.WriteLine(configurationContent1);

            ShowConfigurationContentClass showClass = new ShowConfigurationContentClass();

            Console.WriteLine(2);
            Task<string> configurationContentTask = showClass.GetConfigurationContentAsync();
            string configurationContent2 = configurationContentTask.Result;
            Console.WriteLine(configurationContent2);
        }
    }
}
