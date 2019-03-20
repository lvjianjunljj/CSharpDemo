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


            // It is the reason of setting int 1, 2, 4, 8... for an enum: 
            // We can combine multiple states with the "|" operator
            EnumTest three = EnumTest.One | EnumTest.One | EnumTest.Two;

            // If there is not [Flags] tab, here will print "3".
            Console.WriteLine(three.ToString());
            foreach (EnumTest e in Enum.GetValues(three.GetType()))
            {
                if (three.HasFlag(e))
                {
                    Console.WriteLine(e);
                }
            }
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

    // I think Flags tab is just make function ToString for this enum can return the string of enum not int.
    [Flags]
    public enum EnumTest
    {
        One = 1,
        Two = 2,
        Three = 4,
        Four = 8
    }
}
