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
            Retrier.Retry(() => RetrierDemoWithoutReturnFunction());
            //string retrierResult1 = Retrier.Retry<string>(() => RetrierDemoWithReturnFunction());
            //string retrierResult2 = Retrier.Retry<string, Exception>(() => RetrierDemoWithReturnFunction());
            //Console.WriteLine(retrierResult1);
            //Console.WriteLine(retrierResult2);
        }

        public static void RetrierDemoWithoutReturnFunction()
        {
            Random random = new Random();
            int tryValue = random.Next(5);
            Console.WriteLine($"tryValue: {tryValue}");
            if (tryValue == SuccessTryValue)
            {
                Console.WriteLine($"Success!!! tryValue: {tryValue}");
            }
            else
            {
                throw new Exception($"RetrierDemoFunction failed!!! retrierCount: {tryValue}");
            }
        }

        public static string RetrierDemoWithReturnFunction()
        {
            Random random = new Random();
            int tryValue = random.Next(5);
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
