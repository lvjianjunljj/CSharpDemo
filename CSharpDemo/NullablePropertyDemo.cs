namespace CSharpDemo
{
    using Newtonsoft.Json.Linq;
    using System;

    public class NullablePropertyDemo
    {
        // There is a issue for this defination: Feature is not available in C# 7.3. Please use language version 8.0 or greater.
        // We can see the fix in the PR:
        // Nullable can be used in some basic type like: int/bool/long/enum.
        public NullableDemoClass? NullableClass { get; }
        public string? NullableString { get; set; }
        public JToken? NullableJToken { get; set; }

        public static void MainMethod()
        {
            NullablePropertyDemo demoObj = new NullablePropertyDemo();
            Console.WriteLine(demoObj.NullableClass.HasValue);
            // string cannot be a nullable object.
            //System.Console.WriteLine(demoObj.NullableString.HasValue);
            Console.WriteLine(demoObj.NullableJToken.HasValues);
        }
    }

    public struct NullableDemoClass
    {

    }
}
