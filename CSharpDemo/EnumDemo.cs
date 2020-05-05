using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class EnumDemo
    {
        public static void MainMethod()
        {
            //EnumParseDemo();
            //TypeDescriptorDemo();
            //TraverseDemo();
            //TraverseAllValueDemo();
            //ExceptionDemo();
            TryParseDemo();
        }

        static void EnumParseDemo()
        {
            TestClass t = new TestClass();
            t.EN1 = En.ONE;
            t.EN2 = En.two;
            t.EN3 = En.three;
            JObject j = JObject.FromObject(t);
            Console.WriteLine(j.ToString());

            string str = "One";
            Console.WriteLine((En)Enum.Parse(typeof(En), str)); // One
            Console.WriteLine((En)Enum.Parse(typeof(En), "12")); // 12
            Console.WriteLine((En)Enum.Parse(typeof(En), "1")); //One
        }

        // Compare with EnumParseDemo
        static void TypeDescriptorDemo()
        {
            string str = "One";
            // We cant convert string to enum using Convert.ChangeType function but can use TypeDescriptor.GetConverter function.
            //Console.WriteLine(Convert.ChangeType(str, typeof(En)));
            Console.WriteLine((En)TypeDescriptor.GetConverter(typeof(En)).ConvertFromString(str));
        }

        static void TraverseDemo()
        {
            // It is the reason of setting int 1, 2, 4, 8... for an enum: 
            // We can combine multiple states with the "|" operator
            EnumTest three = EnumTest.One | EnumTest.One | EnumTest.Two;

            // If there is not [Flags] tab, here will print "3".
            Console.WriteLine(three.ToString());
            foreach (EnumTest e in Enum.GetValues(three.GetType()))
            {
                if (three.HasFlag(e))
                {
                    Console.WriteLine(e);
                }
            }
        }

        static void TraverseAllValueDemo()
        {
            foreach (EnumTest e in Enum.GetValues(typeof(EnumTest)))
            {
                Console.WriteLine(e);
            }

            EnumTest three = EnumTest.One | EnumTest.One | EnumTest.Two;
            foreach (EnumTest e in Enum.GetValues(three.GetType()))
            {
                Console.WriteLine(e);
            }
        }

        static void ExceptionDemo()
        {
            try
            {
                // If the value is a integer string, it won't throw exception
                Console.WriteLine((EnumTest)Enum.Parse(typeof(EnumTest), "test"));
            }
            catch (ArgumentException ae)
            {
                Console.WriteLine($"Throw exception mesage: {ae.Message}");
            }
        }

        static void TryParseDemo()
        {
            string enString = "";
            Console.WriteLine(Enum.TryParse<En>(enString, out En enEnum) == false);
            Console.WriteLine(enEnum);
        }
    }

    public class TestClass
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public En EN1 { get; set; }
        public En EN2 { get; set; }
        public En EN3 { get; set; }

    }

    [DefaultValue(En.two)]
    public enum En
    {
        ONE,
        One,
        two,
        three
    }

    // I think Flags tab is just make function ToString for this enum can return the string of enum not int.
    [Flags]
    public enum EnumTest
    {
        One = 1,
        Two = 2,
        Three = 4,
        Four = 8
    }
}
