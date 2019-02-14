using System;
using System.Configuration;
using System.Threading.Tasks;
namespace CSharpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Test();

            Console.ReadKey();
        }

        public async Task<string> TestTask2()
        {
            string resStr = await TestTask();
            return resStr;
        }
        public async Task<string> TestTask()
        {
            return "test";
        }
        public static string Test()
        {
            string res = ConfigurationManager.AppSettings.Get("AppKey");
            Console.WriteLine(ConfigurationManager.AppSettings.Get("AppKey"));
            return res;
        }
    }
}
