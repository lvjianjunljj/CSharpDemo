﻿using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace CSharpDemo.Json
{
    public class JsonCovertDemo
    {
        public static void MainMethod()
        {
            //TypeDemo();
            //StringDemo();
            //DateTimeDeserializeDemo();
            //DoubleDeserializeDemo();
            //EnumDeserializeDemo();
            HasValueDeserializeDemo();
            //ParentClassDemo();
            //DictionaryDemo();
            //DuplicatedPropertyDemo();
        }

        public static void TypeDemo()
        {
            string typeConvertDemoClassStr = "{'Type1':'System.Boolean'}";
            var typeConvertDemo = JsonConvert.DeserializeObject<TypeConvertDemoClass>(typeConvertDemoClassStr);
            Console.WriteLine("Get the actual type of Type1: {0}", typeConvertDemo.Type1);
        }

        public static void StringDemo()
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore,
            };

            string stringConvertDemoClassStr = "{'tEstStr':'tEstStr', 'nullDefinedStr':null, 'EmptyStr':'', 'TrimStr':'  TrimStr    '}";
            // When we call DeserializeObject<T> function, ignore the case of property name by default
            // I think we can set the conifg by the input JsonSerializerSettings
            StringConvertDemoClass stringConvertDemo = JsonConvert.DeserializeObject<StringConvertDemoClass>(stringConvertDemoClassStr, settings);
            JObject stringConvertDemoJObject = JsonConvert.DeserializeObject<JObject>(stringConvertDemoClassStr, settings);
            Console.WriteLine(JsonConvert.SerializeObject(stringConvertDemoJObject, settings));

            Console.WriteLine(stringConvertDemo.NullStr == null);
            Console.WriteLine(stringConvertDemo.NullDefinedStr == null);
            Console.WriteLine(stringConvertDemoJObject);
            Console.WriteLine(stringConvertDemoJObject["nullStr"] == null);


            // Output is false true
            // It is very interesting
            Console.WriteLine(stringConvertDemoJObject["nullDefinedStr"] == null);
            Console.WriteLine(stringConvertDemoJObject["nullDefinedStr"].ToString() == "");

            Console.WriteLine(stringConvertDemoJObject["EmptyStr"].ToString() == "");

            Console.WriteLine(stringConvertDemoJObject["nullStr"] ?? stringConvertDemoJObject["tEstStr"]);
            Console.WriteLine(stringConvertDemoJObject["nullDefinedStr"] ?? stringConvertDemoJObject["tEstStr"]);

            Console.WriteLine(stringConvertDemo.TestStr);
            Console.WriteLine(stringConvertDemo.NullStr ?? stringConvertDemo.TestStr);
            Console.WriteLine(stringConvertDemo.NullDefinedStr ?? stringConvertDemo.TestStr);
            Console.WriteLine(stringConvertDemo.TrimStr);


            Console.WriteLine(JsonConvert.SerializeObject(stringConvertDemo));
            Console.WriteLine(JsonConvert.SerializeObject(stringConvertDemo, settings));
        }

        public static void DateTimeDeserializeDemo()
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

        public static void DoubleDeserializeDemo()
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore,
            };

            string doubleConvertDemoClassStr = "{'Num1':9.2233720368547758E+18, 'Num2':1234}";
            DoubleConvertDemoClass doubleConvertDemoClass = JsonConvert.DeserializeObject<DoubleConvertDemoClass>(doubleConvertDemoClassStr, settings);
            Console.WriteLine(doubleConvertDemoClass.Num1);
            Console.WriteLine(doubleConvertDemoClass.Num2);
        }

        public static void HasValueDeserializeDemo()
        {
            Console.WriteLine("Test for not setting value");
            string hasValueConvertDemoClassStr = "{'Num2':1234}";
            HasValueConvertDemoClass hasValueConvertDemoClass = JsonConvert.DeserializeObject<HasValueConvertDemoClass>(hasValueConvertDemoClassStr);
            Console.WriteLine($"Num1 hasValue: {hasValueConvertDemoClass.Num1.HasValue}, Num1: {hasValueConvertDemoClass.Num1}");
            Console.WriteLine($"Num2 hasValue: {hasValueConvertDemoClass.Num2.HasValue}, Num2: {hasValueConvertDemoClass.Num2}");
            Console.WriteLine($"Num: {hasValueConvertDemoClass.Num1 ?? hasValueConvertDemoClass.Num2}");

            Console.WriteLine("``````````````````");
            Console.WriteLine("Test for setting null value");
            hasValueConvertDemoClassStr = "{'Num1':null，, 'Num2':1234}";
            try
            {
                hasValueConvertDemoClass = JsonConvert.DeserializeObject<HasValueConvertDemoClass>(hasValueConvertDemoClassStr);
                Console.WriteLine($"Num1 hasValue: {hasValueConvertDemoClass.Num1.HasValue}, Num1: {hasValueConvertDemoClass.Num1}");
                Console.WriteLine($"Num2 hasValue: {hasValueConvertDemoClass.Num2.HasValue}, Num2: {hasValueConvertDemoClass.Num2}");
                Console.WriteLine($"Num: {hasValueConvertDemoClass.Num1 ?? hasValueConvertDemoClass.Num2}");
            }
            catch (JsonReaderException e)
            {
                // Cannot set it as null
                Console.WriteLine($"Error message: {e.Message}");
            }
        }

        public static void EnumDeserializeDemo()
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                MissingMemberHandling = MissingMemberHandling.Ignore,
            };

            // Wrong value for enum type
            try
            {
                string doubleConvertDemoClassStr = "{'EnumTestProperty': 'TT'}";
                // Newtonsoft.Json.JsonSerializationException: 
                // 'Error converting value "TT" to type 'CSharpDemo.Json.EnumTest'. Path 'EnumTestProperty', line 1, position 25.'
                EnumConvertDemoClass doubleConvertDemoClass = JsonConvert.DeserializeObject<EnumConvertDemoClass>(doubleConvertDemoClassStr, settings);
                Console.WriteLine(doubleConvertDemoClass.EnumTestProperty);
            }
            catch (JsonSerializationException e)
            {
                Console.WriteLine($"Error message: {e.Message}");
            }

            // Null value for enum type
            try
            {
                string doubleConvertDemoClassStr = "{'EnumTestProperty': null}";
                // Newtonsoft.Json.JsonSerializationException: 
                // 'Error converting value "TT" to type 'CSharpDemo.Json.EnumTest'. Path 'EnumTestProperty', line 1, position 25.'
                EnumConvertDemoClass doubleConvertDemoClass = JsonConvert.DeserializeObject<EnumConvertDemoClass>(doubleConvertDemoClassStr, settings);
                Console.WriteLine(doubleConvertDemoClass.EnumTestProperty);
            }
            catch (JsonSerializationException e)
            {
                Console.WriteLine($"Error message: {e.Message}");
            }

            // Not set for enum type
            try
            {
                string doubleConvertDemoClassStr = "{'EnumTestProperty2': null}";
                // Newtonsoft.Json.JsonSerializationException: 
                // 'Error converting value "TT" to type 'CSharpDemo.Json.EnumTest'. Path 'EnumTestProperty', line 1, position 25.'
                EnumConvertDemoClass doubleConvertDemoClass = JsonConvert.DeserializeObject<EnumConvertDemoClass>(doubleConvertDemoClassStr, settings);
                Console.WriteLine(doubleConvertDemoClass.EnumTestProperty);
            }
            catch (JsonSerializationException e)
            {
                Console.WriteLine($"Error message: {e.Message}");
            }
        }

        // It is impossible to deserialize to parent class and then convert it to target class,
        // Right way is to deserialize to JToken then use ToObejct<T> function to convert it to target class.
        public static void ParentClassDemo()
        {
            string typeConvertDemoClassStr = "{'Type1':'System.Boolean'}";
            var objectClass = JsonConvert.DeserializeObject<JToken>(typeConvertDemoClassStr);
            var typeClass = objectClass.ToObject<TypeConvertDemoClass>();
            Console.WriteLine("Get the actual type of Type1: {0}", typeClass.Type1);

            // Two wrong ways.
            //var objectClass = JsonConvert.DeserializeObject<object>(typeConvertDemoClassStr);
            //var typeClass = objectClass as TypeConvertDemoClass;
            //var typeClass = (TypeConvertDemoClass)Convert.ChangeType(objectClass, typeof(TypeConvertDemoClass));
        }

        public static void DictionaryDemo()
        {
            DictionaryClass dictClass = new DictionaryClass();
            dictClass.Dict = new Dictionary<string, string>()
            {
                ["Key"] = "Value"
            };

            Console.WriteLine(JsonConvert.SerializeObject(dictClass));

            string dictClassStr = "{Dict:{\"Key\":\"Value\"}}";
            var dictParse = JsonConvert.DeserializeObject<DictionaryClass>(dictClassStr);
            foreach (var pair in dictParse.Dict)
            {
                Console.WriteLine($"'{pair.Key}': '{pair.Value}'");

            }
        }

        public static void DuplicatedPropertyDemo()
        {
            string str = "{'TestStr':'TestStr1','TestStr':null,'TestStr':'TestStr2'}";
            var demoOjbect = JsonConvert.DeserializeObject<StringConvertDemoClass>(str);

            Console.WriteLine($"The value of property 'TestStr' for 'DuplicatedPropertyDemo' is: {demoOjbect.TestStr}");
        }
    }

    class StringConvertDemoClass
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

    class DoubleConvertDemoClass
    {
        public double Num1 { get; set; }
        public double Num2 { get; set; }
    }

    class HasValueConvertDemoClass
    {
        public long? Num1 { get; set; }
        public long? Num2 { get; set; }
    }

    class TypeConvertDemoClass
    {
        public Type Type1 { get; set; }
    }
    class EnumConvertDemoClass
    {
        [JsonProperty]
        [JsonConverter(typeof(StringEnumConverter))]
        public EnumTest EnumTestProperty { get; set; }
    }
    enum EnumTest
    {
        T1 = 0,
        T2 = 1,
    }

    class DictionaryClass
    {
        public Dictionary<string, string> Dict { get; set; }
    }
}
