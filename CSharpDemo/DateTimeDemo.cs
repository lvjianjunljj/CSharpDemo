using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class DateTimeDemo
    {
        public static void MainMethod()
        {
            Console.WriteLine(DateTime.UtcNow.ToString("o"));
            Console.WriteLine(DateTime.UtcNow.ToString("r"));
        }
    }
}
