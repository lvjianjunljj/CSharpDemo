using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.SingletonDemo
{
    class StaticVariableDemo
    {
        public static void MainMethod()
        {
            Console.WriteLine("Starting Main");
            Test1.EchoAndReturn("Echo!");
            Console.WriteLine("After echo");
            //Reference a static field in Test
            string y = Test1.x;
            //Use the value just to avoid compiler cleverness
            if (y != null)
            {
                Console.WriteLine("After field access");
            }

            Console.ReadKey();
        }
    }
    class Test1
    {
        public static string x = EchoAndReturn("In type initializer");

        public static string EchoAndReturn(string s)
        {
            Console.WriteLine(s);
            return s;
        }
    }
}
