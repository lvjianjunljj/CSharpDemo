using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.CSharpInDepth.Part2.CSharp2
{
    public class NullableDemo
    {
        public static void MainMethod()
        {
            // This is an interesting feature, especially the result of "a >= b";
            int? a = null;
            int? b = null;
            Console.WriteLine(a);
            Console.WriteLine(a == b);
            Console.WriteLine(a >= b);
            Console.WriteLine(a += b);
        }
    }
}
