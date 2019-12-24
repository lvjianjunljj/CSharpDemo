namespace CSharpDemo
{
    using Newtonsoft.Json;
    using System;
    using System.ComponentModel;

    class DynamicDemo
    {
        public static void MainMethod()
        {
            DynamicParseComparisonDemo();
            //DynamicParseDemo();
            //VariablesDemo();
            //TypeParametersDemo();
        }

        public static void DynamicParseDemo()
        {
            string jsonString = "{'D1': '', 'D2': true}";
            var dynamicParseClass = JsonConvert.DeserializeObject<DynamicParseClass>(jsonString);
            Console.WriteLine("Get the actual type of D1: {0}", dynamicParseClass.D1.GetType().ToString());
            Console.WriteLine("Get the actual type of D2: {0}", dynamicParseClass.D2.GetType().ToString());
        }

        public static void DynamicParseComparisonDemo()
        {
            string targetValueString = "true";
            string targetTypeString = "System.Boolean";
            string jsonString = "{'D1': '', 'D2': true}";

            var dynamicParseClass = JsonConvert.DeserializeObject<DynamicParseClass>(jsonString);
            Console.WriteLine("Get the actual type of D1: {0}", dynamicParseClass.D1.GetType().ToString());
            Console.WriteLine("Get the actual type of D2: {0}", dynamicParseClass.D2.GetType().ToString());
            // The scheam of targetTypeString is "NameSpace.MyClasse", so this line need to be written as:
            // Type.GetType("NameSpace.MyClasse");
            var targetType = Type.GetType(targetTypeString);
            var targetValue = TypeDescriptor.GetConverter(targetType).ConvertFromString(targetValueString);
            //Console.WriteLine(targetValue.GetType());
            Console.WriteLine($"D2 equals to target value: {targetValue.Equals(dynamicParseClass.D2)}");
        }

        public static void VariablesDemo()
        {
            Console.WriteLine("Dynamic variables Demo");
            // Dynamic variables 
            dynamic value1 = "GeeksforGeeks";
            dynamic value2 = 123234;
            dynamic value3 = 2132.55;
            dynamic value4 = false;

            // Get the actual type of  
            // dynamic variables 
            // Using GetType() method 
            Console.WriteLine("Get the actual type of value1: {0}", value1.GetType().ToString());
            Console.WriteLine("Get the actual type of value2: {0}", value2.GetType().ToString());
            Console.WriteLine("Get the actual type of value3: {0}", value3.GetType().ToString());
            Console.WriteLine("Get the actual type of value4: {0}", value4.GetType().ToString());
        }

        public static void TypeParametersDemo()
        {
            Console.WriteLine("Dynamic Type Parameters Demo.");
            // Calling addstr method 
            addstr("G", "G");
            addstr("Geeks", "forGeeks");
            addstr("Cat", "Dog");
            addstr("Hello", 1232);
            addstr(12, 30);
        }

        public static void addstr(dynamic s1, dynamic s2)
        {
            Console.WriteLine(s1 + s2);
        }
    }

    class DynamicParseClass
    {

        public dynamic D1 { get; set; }
        public dynamic D2 { get; set; }

    }
}
