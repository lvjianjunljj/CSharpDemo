using System;
using System.Reflection;

namespace CSharpDemo.ReflectionDemo
{
    class ToStringGetPropertiesDemo
    {
        static void MainMethod()
        {
            TestClass t = new TestClass();
            t.A = "a";
            t.B = "bb";
            t.C = "ccc";
            t.D = "dddd";
            Console.WriteLine(t);
        }
    }
    class TestClass
    {
        public string A { get; set; }
        public string B { get; set; }
        public string C { get; set; }
        public string D { get; set; }

        public override string ToString()
        {
            string returnStr = "{\n";
            Type testRunType = this.GetType();
            PropertyInfo[] propertyInfo = testRunType.GetProperties();
            foreach (PropertyInfo testRunProperty in propertyInfo)
            {
                string bgColorSet = string.Empty;
                string testRunPropertyName = testRunProperty.Name;
                object testRunPropertyValueObject = testRunProperty.GetValue(this);
                string testRunPropertyValueString = testRunPropertyValueObject == null ?
                    string.Empty : testRunPropertyValueObject.ToString();

                returnStr += $"{testRunPropertyName}: {testRunPropertyValueString}\n";
            }
            return returnStr + "}";
        }
    }
}
