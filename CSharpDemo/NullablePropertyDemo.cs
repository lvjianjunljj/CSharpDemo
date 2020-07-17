namespace CSharpDemo
{
    using Newtonsoft.Json.Linq;
    using System;

    public class NullablePropertyDemo
    {
        // There is a issue for this defination: Feature is not available in C# 7.3. Please use language version 8.0 or greater.
        // We can see the fix in the PR: https://github.com/lvjianjunljj/CSharpDemo/commit/dfacdc2da72733d70f514b79b11f8a3a0e6e4f2b
        // Nullable can be used in some basic type like: int/bool/long/enum.
        public NullableDemoStruct? NullableStruct { get; }
        public NullableDemoClass? NullableClass { get; }
        public string? NullableString { get; set; }
        public string NonNullableString { get; set; }
        public JToken? NullableJToken { get; set; }
        public NullableDemoEnum? NullableEnum { get; set; }

        public static void MainMethod()
        {
            NullablePropertyDemo demoObj = new NullablePropertyDemo();

            Console.WriteLine(demoObj.NullableStruct.HasValue);
            // Class defined by user and string cannot be a nullable object.
            //Console.WriteLine(demoObj.NullableClass.HasValue);
            //Console.WriteLine(demoObj.NullableString.HasValue);
            //Console.WriteLine(demoObj.NullableJToken.HasValue);

            Console.WriteLine(demoObj.NullableClass == null);
            // There is no difference between NullableString and NonNullableString
            Console.WriteLine(demoObj.NullableString == null);
            Console.WriteLine(demoObj.NonNullableString == null);
            Console.WriteLine(demoObj.NullableJToken == null);

            Console.WriteLine(demoObj.NullableEnum.HasValue);
            Console.WriteLine(string.IsNullOrEmpty(demoObj.NullableEnum.ToString()));
            try
            {
                Console.WriteLine((NullableDemoEnum)Enum.Parse(typeof(NullableDemoEnum), demoObj.NullableEnum.ToString()));
            }
            catch (Exception e)
            {
                // Need to cathch the exception when we use Enum.Parse in a nullable enum.
                // Exception message: Must specify valid information for parsing in the string.
                Console.WriteLine(e.Message);
            }

            // It is a checker.
            string nullableEnumString = demoObj.NullableEnum.ToString();
            NullableDemoEnum nullableDemoEnum = string.IsNullOrEmpty(nullableEnumString) ? NullableDemoEnum.None : (NullableDemoEnum)Enum.Parse(typeof(NullableDemoEnum), nullableEnumString);
            Console.WriteLine(nullableDemoEnum.ToString());

            // A better checker.
            nullableDemoEnum = demoObj.NullableEnum.HasValue ?
                (NullableDemoEnum)Enum.Parse(typeof(NullableDemoEnum), demoObj.NullableEnum.ToString()) :
                NullableDemoEnum.None;
            Console.WriteLine(nullableDemoEnum.ToString());

            NullableDemoEnum? nullNullableDemoEnum = demoObj.NullableEnum.HasValue ?
                (Nullable<NullableDemoEnum>)Enum.Parse(typeof(NullableDemoEnum), demoObj.NullableEnum.ToString()) :
                null;
            Console.WriteLine(nullNullableDemoEnum == null);

            Console.WriteLine("End..");
        }
    }

    public struct NullableDemoStruct
    {

    }
    public class NullableDemoClass
    {

    }

    public enum NullableDemoEnum
    {
        None = 0,
        One = 1,
        Two = 2
    }
}
