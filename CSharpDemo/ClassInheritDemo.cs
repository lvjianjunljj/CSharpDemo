namespace CSharpDemo
{
    using System;
    using Newtonsoft.Json;

    public class ClassInheritDemo
    {
        public static void MainMethod()
        {
            ParentClass parentInstance = new ChildClass { B = "B" };
            parentInstance.A = "A";
            if (parentInstance.GetType() == typeof(ParentClass))
            {
                Console.WriteLine("Type of parentInstance is ParentClass");
            }
            else
            {
                Console.WriteLine("Type of parentInstance is ChildClass");
            }

            // Key word "is" is different from the prevous if check logic
            Console.WriteLine(parentInstance is ParentClass);
            Console.WriteLine(parentInstance is ChildClass);
            string parentString = JsonConvert.SerializeObject(parentInstance);
            Console.WriteLine(parentString);

            Console.WriteLine("Deserialize parent string to ParentClass...");
            parentInstance = JsonConvert.DeserializeObject<ParentClass>(parentString);
            if (parentInstance.GetType() == typeof(ParentClass))
            {
                Console.WriteLine("p");
            }
            else
            {
                Console.WriteLine("c");
            }

            Console.WriteLine(parentInstance is ParentClass);
            Console.WriteLine(parentInstance is ChildClass);
            Console.WriteLine(JsonConvert.SerializeObject(parentInstance));

            Console.WriteLine("Deserialize parent string to ChildClass...");
            parentInstance = JsonConvert.DeserializeObject<ChildClass>(parentString);
            if (parentInstance.GetType() == typeof(ParentClass))
            {
                Console.WriteLine("Type of parentInstance is ParentClass");
            }
            else
            {
                Console.WriteLine("Type of parentInstance is ChildClass");
            }

            Console.WriteLine(parentInstance is ParentClass);
            Console.WriteLine(parentInstance is ChildClass);

            Console.WriteLine(JsonConvert.SerializeObject(parentInstance));
        }
    }

    class ParentClass
    {
        public string A { get; set; }
    }

    class ChildClass : ParentClass
    {
        public string B { get; set; }
    }
}
