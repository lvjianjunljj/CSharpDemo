using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class StringDemo
    {
        public static void MainMethod()
        {
            DateTime a = DateTime.Now;
            // ToString Tips: The two method are the same
            Console.WriteLine($"{a:t}");
            Console.WriteLine($"{a.ToString("t")}");
        }
    }
}
