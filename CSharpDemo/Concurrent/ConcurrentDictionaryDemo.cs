// The doc link: https://docs.microsoft.com/en-us/dotnet/api/system.collections.concurrent.concurrentdictionary-2.getoradd?view=netcore-3.1

namespace CSharpDemo.Concurrent
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;

    public class ConcurrentDictionaryDemo
    {
        public static void MainMethod()
        {
            LazyConcurrentDictionaryDemo();
        }

        // The doc is in the link: 
        // https://endjin.com/blog/2015/10/using-lazy-and-concurrentdictionary-to-ensure-a-thread-safe-run-once-lazy-loaded-collection
        private static void LazyConcurrentDictionaryDemo()
        {
            // Won't update the value for the key we have added.
            LazyConcurrentDictionary<string, string> dict = new LazyConcurrentDictionary<string, string>();
            Console.WriteLine(dict.GetOrAdd("1", (key) => key + 1));
            Console.WriteLine(dict.GetOrAdd("1", (key) => key + 2));
            Console.WriteLine(dict.GetOrAdd("1", (key) => key));
        }
    }

    // Created a **LazyConcurrentDictionary **to wrap up the **GetOrAdd **method
    // We just can see the difference when multi-parallel
    class LazyConcurrentDictionary<TKey, TValue>
    {
        private readonly ConcurrentDictionary<TKey, Lazy<TValue>> concurrentDictionary;

        public LazyConcurrentDictionary()
        {
            this.concurrentDictionary = new ConcurrentDictionary<TKey, Lazy<TValue>>();
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            var lazyResult = this.concurrentDictionary.GetOrAdd(key, k => new Lazy<TValue>(() => valueFactory(k), LazyThreadSafetyMode.ExecutionAndPublication));

            return lazyResult.Value;
        }
    }
}
