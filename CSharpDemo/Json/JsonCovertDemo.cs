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
            Console.WriteLine(JsonConvert.SerializeObject(a));


            string dateString = "0001-01-01T00:00:00";

            // But for DateTime, we cannot use JsonConvert to convert string to DateTime but need to use DateTime.Parse
            //DateTime dateTime = JsonConvert.DeserializeObject<DateTime>(dateString);

            DateTime dateTime = DateTime.Parse(dateString);
            Console.WriteLine(dateTime.ToString("o"));
        }
    }
}
