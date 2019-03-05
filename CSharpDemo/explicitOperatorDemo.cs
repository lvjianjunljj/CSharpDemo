using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class ExplicitOperatorDemo
    {
        static void MainMethod()
        {
            string str = "qwer";
            ExplicitOperatorDemo ex = (ExplicitOperatorDemo)str;
            Console.WriteLine(ex.ToString());
        }
        public string Attribute1 { get; set; }
        public string Attribute2 { get; set; }

        // override method executed during type conversion
        public static explicit operator ExplicitOperatorDemo(string inputStr)
        {
            ExplicitOperatorDemo res = new ExplicitOperatorDemo();
            res.Attribute1 = inputStr;
            res.Attribute2 = inputStr + inputStr;
            return res;
        }

        public override string ToString() => Attribute1 + "_" + Attribute2;
    }
}
