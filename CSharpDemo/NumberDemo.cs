using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class NumberDemo
    {
        static void MainMethod()
        {
            long a = -11;
            ulong b = 2;
            Console.WriteLine((ulong)a);
            // Make sure A is greater than or equal to 0
            Console.WriteLine((ulong)a < b);
            Console.WriteLine(Int32.MaxValue < UInt32.MaxValue);
            Console.WriteLine(UInt32.MaxValue);
            Console.WriteLine(Int64.MaxValue);
            Console.WriteLine(UInt64.MaxValue);
            Console.WriteLine(int.MaxValue);
            Console.WriteLine(long.MaxValue);
            Console.WriteLine(long.MaxValue < ulong.Parse("9223372036854776000"));
        }
    }
}
