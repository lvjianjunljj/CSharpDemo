using System;
using System.Collections.Generic;
using CSharpDemo.Azure;
using CSharpDemo.DIStudy.LifetimeScopeControl;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo
{
    class JTokenTestDemo
    {
        static void MainMethod(string[] args)
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
            Console.ReadKey();
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
        public JTokenTest2(string d)
        {
            this.d = d;
        }
    }
}