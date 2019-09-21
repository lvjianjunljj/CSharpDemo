using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;

namespace CSharpDemo.Json
{
    class JsonCovertDemo
    {
        public static void MainMethod()
        {
            StringTest();
            //DateTimeDeserializeTest();
        }

        public static void StringTest()
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore,
            };

            string texString = "{'tEstStr':'tEstStr', 'nullDefinedStr':null, 'EmptyStr':'', 'TrimStr':'  TrimStr    '}";
            // When we call DeserializeObject<T> function, ignore the case of property name by default
            // I think we can set the conifg by the input JsonSerializerSettings
            Tex tex = JsonConvert.DeserializeObject<Tex>(texString, settings);
            JObject texJObject = JsonConvert.DeserializeObject<JObject>(texString, settings);
            Console.WriteLine(JsonConvert.SerializeObject(texJObject, settings));

            Console.WriteLine(tex.NullStr == null);
            Console.WriteLine(tex.NullDefinedStr == null);
            Console.WriteLine(texJObject);
            Console.WriteLine(texJObject["nullStr"] == null);


            // Output is false true
            // It is very interesting
            Console.WriteLine(texJObject["nullDefinedStr"] == null);
            Console.WriteLine(texJObject["nullDefinedStr"].ToString() == "");

            Console.WriteLine(texJObject["EmptyStr"].ToString() == "");

            Console.WriteLine(texJObject["nullStr"] ?? texJObject["tEstStr"]);
            Console.WriteLine(texJObject["nullDefinedStr"] ?? texJObject["tEstStr"]);

            Console.WriteLine(tex.TestStr);
            Console.WriteLine(tex.NullStr ?? tex.TestStr);
            Console.WriteLine(tex.NullDefinedStr ?? tex.TestStr);
            Console.WriteLine(tex.TrimStr);


            Console.WriteLine(JsonConvert.SerializeObject(tex));
            Console.WriteLine(JsonConvert.SerializeObject(tex, settings));
        }

        class Tex
        {
            private string trimStr;
            public string TestStr { get; set; }
            public string NullStr { get; set; }
            public string NullDefinedStr { get; set; }
            public string EmptyStr { get; set; }

            public string TrimStr
            {
                get
                {
                    return this.trimStr;
                }
                set
                {
                    this.trimStr = value.Trim();
                }
            }
        }

        public void DateTimeDeserializeTest()
        {
            string dateString = "0001-01-01T00:00:00";

            // But for DateTime, we cannot use JsonConvert to convert string to DateTime but need to use DateTime.Parse
            //DateTime dateTime = JsonConvert.DeserializeObject<DateTime>(dateString);

            DateTime dateTime = DateTime.Parse(dateString);

            // These is the two main schema for dataTime I know.
            // Doc link: https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings
            Console.WriteLine(dateTime.ToString("o"));
            Console.WriteLine(dateTime.ToString("r"));
        }
    }
}
