using Newtonsoft.Json;
using System;

namespace CSharpDemo.Json
{
    class JsonCovertDemo
    {
        class Tex
        {
            public string a { get; set; }
        }
        static void Run()
        {
            Tex a = JsonConvert.DeserializeObject<Tex>("{'a':'b'}");
            Console.WriteLine(a.a);
        }
    }
}
