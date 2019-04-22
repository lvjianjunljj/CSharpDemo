using System;
using System.ComponentModel;
using System.Reflection;

namespace CSharpDemo.ReflectionDemo
{
    class ReflectionSetValue
    {
        public static void MainMethod()
        {
            Test t = new Test();
            Type type = t.GetType();
            PropertyInfo propertyInfo = type.GetProperty("A");
            propertyInfo.SetValue(t, "AAA");
            propertyInfo = type.GetProperty("B");
            Console.WriteLine($"property value: {propertyInfo?.GetValue(t)}");
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
            propertyInfo?.SetValue(t, TypeDescriptor.GetConverter(propertyInfo.PropertyType).ConvertFromString("Two"));

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
    }
    enum EnumClass
    {
        Zero = 0,
        One = 1,
        Two = 2,
        Four = 4
    }
}