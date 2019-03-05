using CSharpDemo.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace CSharpDemo
{
    class AttributesDemo
    {
        static void MainMethod()
        {
            Test t = new Test();
            t.T1 = "tt1";
            t.T2 = "tt2";
            t.T3 = "tt3";


            string a = JsonConvert.SerializeObject(t);
            JToken j = JToken.FromObject("1:1");
            Console.WriteLine(a);


            Type type = t.GetType();
            PropertyInfo[] propertyInfo = type.GetProperties();

            foreach (PropertyInfo p in propertyInfo)
            {

                string propertyName = p.Name;
                string propertyValue = p.GetValue(t).ToString();
                Console.WriteLine("propertyName------>" + propertyName);
                Console.WriteLine("propertyValue----->" + propertyValue);
                foreach (HtmlStyleAttribute attr in p.GetCustomAttributes(typeof(HtmlStyleAttribute)))
                {
                    Console.WriteLine(attr.Hidden);
                    foreach (string str in attr.BgColorMapping.Keys) Console.WriteLine(attr.BgColorMapping[str]); ;
                }
                Console.WriteLine("-------------------------------------------------------------------");

            }

            Console.ReadKey();
        }
    }

    public class Test
    {
        [JsonProperty(PropertyName = "tt1")]
        [HtmlStyle(false, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")]
        public string T1 { get; set; }
        [JsonProperty(PropertyName = "tt2")]
        [HtmlStyle(true)]
        public string T2 { get; set; }
        [JsonProperty(PropertyName = "tt3")]
        public string T3 { get; set; }
    }
    class HtmlStyleAttribute : Attribute
    {
        public bool Hidden { get; set; }
        public Dictionary<string, string> BgColorMapping { get; set; }

        // Attribute constructor parameter cannot be the class in System.Collections.Generic
        // such as List<T>, Set<T> or Dictionary<TKey, TValue>.
        // So I think this is the simplest way for the requirement of Dictionary.
        public HtmlStyleAttribute(bool hidden, params string[] bgColorMapping)
        {
            this.Hidden = hidden;
            this.BgColorMapping = new Dictionary<string, string>();
            for (int i = 0; i < bgColorMapping.Length - 1; i += 2)
            {
                this.BgColorMapping.Add(bgColorMapping[i], bgColorMapping[i + 1]);
            }
        }
    }

}