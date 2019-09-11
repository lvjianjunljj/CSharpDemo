using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.Json
{
    public class JsonParse
    {
        public static void MainMethod()
        {
            //JArrayParseTest();
            //DateTimeParseTest();
        }

        public static void JArrayParseTest()
        {
            // The result is the same.
            // string apiText = @"{""1234"":""ava"", ""list"":[""1"",""2"",""3""]}";
            string apiText = @"{1234:""ava"", list:[1,2,3]}";
            try
            {
                JObject jsonObj = JObject.Parse(apiText);
                Console.WriteLine(jsonObj["1234"]);
                JArray jlist = JArray.Parse(jsonObj["list"].ToString());
                for (int i = 0; i < jlist.Count; i++)
                    Console.WriteLine((int)jlist[i]);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public static void DateTimeParseTest()
        {
            string timeSpanTestClassStr = "{TimeSpanTest: '24:00:00', DateTimeTest: '2019-08-22T00:00:00Z'}";


            Console.WriteLine("JObject.Parse Function...");
            JObject timeSpanTestClassJObject = JObject.Parse(timeSpanTestClassStr);

            Console.WriteLine(timeSpanTestClassJObject["TimeSpanTest"]);
            // This output is not "2019-08-22T00:00:00Z", so interesting
            Console.WriteLine(timeSpanTestClassJObject["DateTimeTest"]);

            Console.WriteLine("JObject.Parse Function...");
            JToken timeSpanTestClassJToken = JToken.Parse(timeSpanTestClassStr);

            Console.WriteLine(timeSpanTestClassJToken["TimeSpanTest"]);
            // This output is not "2019-08-22T00:00:00Z", so interesting
            Console.WriteLine(timeSpanTestClassJToken["DateTimeTest"]);

            Console.WriteLine("JsonConvert.DeserializeObject<JObject> Function...");
            var settings = new JsonSerializerSettings
            {
                // Default value is DateParseHandling.DateTime
                DateParseHandling = DateParseHandling.None
            };
            JObject timeSpanTestClassJsonConvert = JsonConvert.DeserializeObject<JObject>(timeSpanTestClassStr, settings);

            Console.WriteLine(timeSpanTestClassJsonConvert["TimeSpanTest"]);
            Console.WriteLine(timeSpanTestClassJsonConvert["DateTimeTest"]);

            Console.WriteLine("Class property...");
            TimeTestClass timeTestClass = timeSpanTestClassJObject.ToObject<TimeTestClass>();

            Console.WriteLine(timeTestClass.TimeSpanTest);
            Console.WriteLine(timeTestClass.DateTimeTest);
            Console.WriteLine(timeTestClass.DateTimeTest.ToString("u"));

            Console.WriteLine(JsonConvert.SerializeObject(timeTestClass));
        }

        class TimeTestClass
        {
            public TimeSpan TimeSpanTest { get; set; }

            public DateTime DateTimeTest { get; set; }

        }
    }
}
