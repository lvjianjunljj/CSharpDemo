using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.RetrierDir
{
    class RetrierDemo
    {
        private static int SuccessTryValue = 0;
        public static void MainMethod()
        {
            string retrierResult = Retrier.Retry<string>(() => RetrierDemoFunction());
            Console.WriteLine(retrierResult);
        }

        public static string RetrierDemoFunction()
        {
            Random random = new Random();
            int tryValue = random.Next(3);
            Console.WriteLine($"tryValue: {tryValue}");
            if (tryValue == SuccessTryValue)
            {
                return $"Success!!! tryValue: {tryValue}";
            }
            else
            {
                throw new Exception($"RetrierDemoFunction failed!!! retrierCount: {tryValue}");
            }
        }
    }
}
