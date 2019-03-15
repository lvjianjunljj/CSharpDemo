using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class EnumDemo
    {
        public static void MainMethod()
        {
            //QueryIncidents.MainMethod();
            //AzureCosmosDB.MainMethod();

            TestClass t = new TestClass();
            t.EN1 = En.one;
            t.EN2 = En.two;
            t.EN3 = En.three;
            JObject j = JObject.FromObject(t);
            Console.WriteLine(j.ToString());

            string str = "one";
            Console.WriteLine((En)Enum.Parse(typeof(En), str));
            Console.WriteLine((En)Enum.Parse(typeof(En), "123"));
            Console.WriteLine((En)Enum.Parse(typeof(En), "1"));
        }
    }
    public class TestClass
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public En EN1 { get; set; }
        public En EN2 { get; set; }
        public En EN3 { get; set; }

    }
    public enum En
    {
        one = 1,
        two = 2,
        three = 3
    }
}
