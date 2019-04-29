using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;

namespace CSharpDemo.ReflectionDemo
{
    class ReflectionSetValue
    {
        public static void MainMethod()
        {
            Test t = new Test();
            t.E = new List<string>();
            t.E.Add("111");
            t.E.Add("222");
            Type type = t.GetType();
            PropertyInfo propertyInfo = type.GetProperty("A");
            propertyInfo.SetValue(t, "AAA");
            propertyInfo = type.GetProperty("Baaa");
            Console.WriteLine(propertyInfo?.GetValue(t).ToString());
            Console.WriteLine($"property value: {propertyInfo?.GetValue(t).ToString()}");
            if (propertyInfo == null)
            {
                Console.WriteLine(1234);
            }
            else
            {
                propertyInfo.SetValue(t, "BBB");
            }
            propertyInfo = type.GetProperty("C");
            propertyInfo.SetValue(t, Convert.ChangeType("1", propertyInfo.PropertyType));
            propertyInfo = type.GetProperty("Enu");
            //if (propertyInfo.PropertyType.IsEnum)
            //{
            //    // Cannot convert string to Enum with Convert.ChangeType function.
            //    propertyInfo.SetValue(t, Enum.Parse(propertyInfo.PropertyType, "One"));
            //}
            //else
            //{
            //    propertyInfo.SetValue(t, Convert.ChangeType("One", propertyInfo.PropertyType));
            //}

            // We can convert string to Enum with Convert.ChangeType function
            propertyInfo?.SetValue(t, TypeDescriptor.GetConverter(propertyInfo.PropertyType).ConvertFromString("Two"));


            // We cant convert string to Collection from string through these function
            propertyInfo = type.GetProperty("E");
            CollectionConverter collectionConverter = new CollectionConverter();
            Console.WriteLine(collectionConverter.ConvertFromString("[1,2,3]"));
            Console.WriteLine(TypeDescriptor.GetConverter(typeof(IList<string>)).ConvertFrom("[1,2,3]"));

            //Console.WriteLine(JsonConvert.DeserializeObject<IList<string>>("[1,2,3]"));
            //propertyInfo?.SetValue(t, TypeDescriptor.GetConverter(propertyInfo.PropertyType).ConvertFromString("[1,2,3]"));
            //Console.WriteLine(t.E[0]);

            //propertyInfo?.SetValue(t, JsonConvert.DeserializeObject<dynamic>("[1,2,3]"));

            try
            {
                propertyInfo = type.GetProperty("D");
                Console.WriteLine(propertyInfo?.GetValue(t).ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine("? not catch all the null: " + e.Message);
            }

            Console.WriteLine(t.A);
            Console.WriteLine(t.Enu);
            Console.WriteLine(t.C);
        }

    }
    class Test
    {
        public string A { get; set; }
        public EnumClass Enu { get; set; }
        public int C { get; set; }
        public string D { get; set; }
        public IList<string> E { get; set; }
    }
    enum EnumClass
    {
        Zero = 0,
        One = 1,
        Two = 2,
        Four = 4
    }
}