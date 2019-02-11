using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class GetStrValue
    {
        static void MainFunction(string[] args)
        {
            GetStrValue p = new GetStrValue();
            p.GetStrValueMethod("test");
            Console.ReadKey();
        }
        void GetStrValueMethod(string str)
        {
            Type type = this.GetType();
            //获取字符串str对应的变量名的变量值
            Console.WriteLine(type.GetField(str, BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this).ToString());
        }
    }
}
