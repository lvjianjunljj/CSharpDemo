using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo
{
    class ConvertDemo
    {
        public static void MainMethod()
        {
            ConvertDemo convertDemo = new ConvertDemo();
            convertDemo.array = new string[] { "1234", "1234" };
            convertDemo.list = new List<string>();
            convertDemo.list.Add("1234");
            Console.WriteLine(JsonConvert.DeserializeObject<IList<string>>(JsonConvert.SerializeObject(convertDemo.list)));
            Console.WriteLine(JsonConvert.DeserializeObject<object[]>(JsonConvert.SerializeObject(convertDemo.array)));


            try
            {
                Console.WriteLine(TypeDescriptor.GetConverter(convertDemo.array.GetType()).ConvertFromString(JsonConvert.SerializeObject(convertDemo.array)));
                Console.WriteLine(TypeDescriptor.GetConverter(convertDemo.list.GetType()).ConvertFromString(JsonConvert.SerializeObject(convertDemo.list)));
            }
            catch (Exception e)
            {
                Console.WriteLine($"Function TypeDescriptor has error: {e.Message}");
            }





            Type type = typeof(Foo); // possibly from a string
            IList list = (IList)Activator.CreateInstance(
                typeof(List<>).MakeGenericType(type));

            object obj = Activator.CreateInstance(type);
            type.GetProperty("Bar").SetValue(obj, "abc", null);
            list.Add(obj);
            Console.WriteLine(list);


            string json = "{\"Id\":1,\"Name\":\"Tom\",\"Age\":\"22\"}";
            //此处模拟在不建实体类的情况下，反转将json返回回dynamic对象
            dynamic DynamicObject = JsonConvert.DeserializeObject<dynamic>(json);
            Console.WriteLine(DynamicObject.Name);  //Tom





            convertDemo.list = new List<string>();
            convertDemo.list.Add("1234");
            convertDemo.list.Add("4321");

            string attrString = JToken.Parse(convertDemo.ToString())["list"].ToString();
            Console.WriteLine(attrString);


            convertDemo.list = new List<string>();
            JToken attrJToken = JToken.Parse(attrString);
            convertDemo.list = attrJToken.ToObject<List<string>>();
            Console.WriteLine(convertDemo.list[1]);
            Console.WriteLine(convertDemo.GetType().GetProperty("list").PropertyType.IsArray);



            Console.ReadKey();
        }

        [JsonProperty("list")]
        public IList<string> list { get; set; }
        public string[] array { get; set; }
        public override string ToString()
        {
            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            return JsonConvert.SerializeObject(this, jsonSerializerSettings);
        }
    }
}

class Foo
{
    public string Bar { get; set; }
}

