namespace CSharpDemo.InvalidCastExceptionDemo
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;

    public class InvalidCastExceptionRunDemo
    {
        public static void MainMethod()
        {
            //ThrowExceptionDemo();
            //NotThrowExceptionDemo();
            //StringConvertDemo();
            StringConvertFixDemo();
        }

        private static void ThrowExceptionDemo()
        {
            try
            {
                BaseClass baseObject = new BaseClass();
                // It will throw InvalidCastException.
                // The root cause is not the properties setting for sub class.
                SubClass subObject = (SubClass)baseObject;
                Console.WriteLine($"RawName: {subObject.RawName}");
            }
            catch (InvalidCastException ice)
            {
                Console.WriteLine($"InvalidCastException message: {ice.Message}");
            }
        }

        private static void NotThrowExceptionDemo()
        {
            BaseClass baseObject = new SubClass();
            // It will not throw InvalidCastException.
            SubClass subObject = (SubClass)baseObject;
            Console.WriteLine($"RawName: {subObject.RawName}");
        }

        // For fixing the bug in DBCore2
        private static void StringConvertDemo()
        {
            BaseClass baseObject = new SubClass()
            {
                Name = "Test Base Object",
                RawName = "Test Raw Name"
            };

            SubClass subObject = new SubClass()
            {
                Name = "Test Sub Object",
                RawName = "Test Raw Name"
            };

            // Do not print this string, 
            // It is not readable
            var stringParse = Convert.ToBase64String(
                System.Text.Encoding.UTF8.GetBytes(
                    JsonConvert.SerializeObject(new List<BaseClass>
                    {
                        baseObject,
                        subObject
                    })));
            var base64String = Convert.FromBase64String(stringParse);
            var objects = JsonConvert.DeserializeObject<List<BaseClass>>(
                    System.Text.Encoding.UTF8.GetString(base64String),
                    new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });

            foreach (var obj in objects)
            {
                Console.WriteLine("Demo object: ");
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(obj)));
                try
                {
                    Console.WriteLine($"RowName: {((SubClass)obj).RawName}");
                }
                catch (InvalidCastException ice)
                {
                    Console.WriteLine($"InvalidCastException message: {ice.Message}");
                }
            }
        }

        private static void StringConvertFixDemo()
        {
            BaseClass baseObject = new SubClass()
            {
                Name = "Test Base Object",
                RawName = "Test Raw Name"
            };

            SubClass subObject = new SubClass()
            {
                Name = "Test Sub Object",
                RawName = "Test Raw Name"
            };

            // Do not print this string, 
            // It is not readable
            var stringParse = Convert.ToBase64String(
                System.Text.Encoding.UTF8.GetBytes(
                    JsonConvert.SerializeObject(new List<BaseClass>
                    {
                        baseObject,
                        subObject
                    })));
            var base64String = Convert.FromBase64String(stringParse);
            var jsons = JsonConvert.DeserializeObject<List<JObject>>(
                    System.Text.Encoding.UTF8.GetString(base64String),
                    new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });

            foreach (var json in jsons)
            {
                Console.WriteLine("Demo object: ");
                Console.WriteLine(json);
                try
                {
                    Console.WriteLine($"RowName: {(json.ToObject<SubClass>()).RawName}");
                }
                catch (InvalidCastException ice)
                {
                    Console.WriteLine($"InvalidCastException message: {ice.Message}");
                }
            }
        }

        private static void Test()
        {
            BaseClass baseObject = new SubClass()
            {
                Name = "Test Base Object",
                RawName = "Test Raw Name"
            };

            // Casting will not create a new object
            Console.WriteLine(baseObject.Name);
            SubClass subObject2 = (SubClass)baseObject;
            subObject2.Name = "Test Sub Object_2";
            Console.WriteLine(baseObject.Name);

            // Outout: 
            // Test Base Object
            // Test Sub Object_2
        }
    }
}
