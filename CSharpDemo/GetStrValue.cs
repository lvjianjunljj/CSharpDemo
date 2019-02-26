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
        string test;
        public static void MainFunction()
        {
            GetStrValue p = new GetStrValue();
            p.GetStrValueMethod("test");
            Console.ReadKey();
        }
        void GetStrValueMethod(string str)
        {
            this.test = "1234";
            Type type = this.GetType();
            //获取字符串str对应的变量名的变量值
            Console.WriteLine(type.GetField(str, BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this).ToString());
        }
    }
}
