using System;
using System.Collections.Generic;
using CSharpDemo.Azure;
using CSharpDemo.DIStudy.LifetimeScopeControl;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.Json
{
    class JTokenTestDemo
    {
        public static void MainMethod()
        {
            //RecursiveAccessProperty();
            TryParseDemo();
        }

        public static void RecursiveAccessProperty()
        {
            JTokenTest t = new JTokenTest();
            t.a = "a";
            t.b = new string[] { "b", "b", "b", "b", "b", "b", "b", "b" };
            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("d", "d");
            t.d = dict;
            t.i = 3;
            t.l = false;
            JTokenTest2 t2 = new JTokenTest2("d");
            t.t = t2;
            t.c = new JTokenTest2[] { t2, t2, t2 };
            //string a = @"\";
            //Console.WriteLine(t);
            JToken jToken = JToken.FromObject(t);
            //Console.WriteLine(jToken.ToString());
            foreach (JProperty property in jToken)
            {
                Console.WriteLine(property.Name);
                JToken value = property.Value;
                Console.WriteLine(value.GetType() == typeof(JValue));
                if (value.GetType() == typeof(JArray))
                {
                    //Console.WriteLine(value);
                    foreach (JToken p in value)
                        Console.WriteLine(p);
                }
                Type ty = value.GetType();
            }
            //Console.WriteLine(jToken["a"]);
        }

        public static void TryParseDemo()
        {
            string jTokenStr = "{'id2':'id2','name2':'name2'}";
            var jToken = JsonConvert.DeserializeObject<JToken>(jTokenStr);
            if (TryParseToJTokenTest(jToken, out TryParseClass1 tryParseClass1, out TryParseClass2 tryParseClass2))
            {
                Console.WriteLine("tryParseClass1.Id1: {0}", tryParseClass1.Id1);
                Console.WriteLine("tryParseClass1.Name1: {0}", tryParseClass1.Name1);
            }
            else
            {
                Console.WriteLine("tryParseClass2.Id2: {0}", tryParseClass2.Id2);
                Console.WriteLine("tryParseClass2.Name2: {0}", tryParseClass2.Name2);
            }
        }

        private static string ConvertTestContentToHtmlString(JToken jTokenContent)
        {
            if (jTokenContent == null)
            {
                throw new ArgumentNullException(nameof(jTokenContent));
            }

            string htmlString = "<table border='2' cellspacing='5'>";
            if (jTokenContent.GetType() == typeof(JObject))
            {
                foreach (JProperty property in jTokenContent)
                {
                    htmlString += $"<tr><th>{property.Name}</th>"
                        + $"<th>{ConvertTestContentToHtmlString(property.Value)}</th></tr>";
                }
            }
            else if (jTokenContent.GetType() == typeof(JArray))
            {
                foreach (JToken jTokenChildContent in jTokenContent)
                {
                    htmlString += $"<tr><th>{ConvertTestContentToHtmlString(jTokenChildContent)}</th></tr>";
                }
            }
            else
            {
                return jTokenContent.ToString();
            }
            return htmlString + "</table>";
        }

        private static bool TryParseToJTokenTest(JToken inputJToken, out TryParseClass1 tryParseClass1, out TryParseClass2 tryParseClass2)
        {
            try
            {
                tryParseClass1 = inputJToken.ToObject<TryParseClass1>();
                tryParseClass2 = null;
                return true;
            }
            catch (JsonSerializationException e)
            {
                Console.WriteLine(e.Message);
                tryParseClass1 = null;
                tryParseClass2 = inputJToken.ToObject<TryParseClass2>();
                return false;
            }
        }

    }
    class JTokenTest
    {
        public string a { get; set; }
        public string[] b { get; set; }
        public JTokenTest2[] c { get; set; }
        public Dictionary<string, string> d { get; set; }
        public JTokenTest2 t { get; set; }
        public int i { get; set; }
        public bool l { get; set; }
        //public override string ToString()
        //{
        //    return JsonConvert.SerializeObject(this);
        //}
    }
    class JTokenTest2
    {
        public string d { get; set; }
        public string str { get; set; }
        public JTokenTest2(string d)
        {
            this.d = d;
        }
    }

    class TryParseClass1
    {
        /// <summary>
        /// Gets or sets the id for this test run
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id1", Required = Required.Always)]
        public string Id1 { get; set; }

        /// <summary>
        /// Gets or sets the tag to indicate who is the last one to update the entity
        /// </summary>
        [JsonProperty(PropertyName = "name1", Required = Required.Always)]
        public string Name1 { get; set; }
    }

    class TryParseClass2
    {
        /// <summary>
        /// Gets or sets the id for this test run
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id2", Required = Required.Always)]
        public string Id2 { get; set; }

        /// <summary>
        /// Gets or sets the tag to indicate who is the last one to update the entity
        /// </summary>
        [JsonProperty(PropertyName = "name2", Required = Required.Always)]
        public string Name2 { get; set; }
    }
}