using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemoAux
{
    public class ShowConfigurationContentClass
    {
        public static string GetConfigurationContent()
        {
            string res = ConfigurationManager.AppSettings.Get("AppKey");
            Console.WriteLine(ConfigurationManager.AppSettings.Get("AppKey"));
            return res;
        }
        public async Task<string> GetConfigurationContentAsync()
        {
            string res = ConfigurationManager.AppSettings.Get("AppKey");
            Console.WriteLine(ConfigurationManager.AppSettings.Get("AppKey"));
            return res;
        }
    }
}
