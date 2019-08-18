using Newtonsoft.Json;
using System;

namespace CSharpDemo.Json
{
    class JsonCovertDemo
    {
        class Tex
        {
            public string TestStr { get; set; }
            public string NullStr { get; set; }
        }
        public static void MainMethod()
        {
            Tex tex = JsonConvert.DeserializeObject<Tex>("{'testStr':'testStr'}");
            Console.WriteLine(tex.TestStr);
            Console.WriteLine(tex.NullStr ?? tex.TestStr);
            Console.WriteLine(JsonConvert.SerializeObject(tex));


            string dateString = "0001-01-01T00:00:00";

            // But for DateTime, we cannot use JsonConvert to convert string to DateTime but need to use DateTime.Parse
            //DateTime dateTime = JsonConvert.DeserializeObject<DateTime>(dateString);

            DateTime dateTime = DateTime.Parse(dateString);
            Console.WriteLine(dateTime.ToString("o"));
        }
    }
}
