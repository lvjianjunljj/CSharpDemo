namespace CSharpDemo.Azure.CosmosDB
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;

    public class AzureCosmosDBTestClass : TestParentClass
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("partitionKey")]
        public string PartitionKey { get; set; }

        [JsonProperty("Testa")]
        public string TestA { get; set; } = "1234";

        [JsonProperty("Testb")]
        public string TestB { get; set; }

        [JsonProperty("Testc")]
        public string TestC { get; set; }

        public bool ShouldSerializeTestC()
        {
            return this.TestC == "c";
        }

        [JsonProperty("Testd")]
        private string TestD { get; set; } = "private testd";

        [JsonProperty("TestHashSet")]
        public HashSet<string> TestHashSet { get; set; }


        [JsonProperty("testLongMaxValueViaLong")]
        public long TestLongMaxValueViaLong { get; set; }

        [JsonProperty("testLongMaxValueViaDouble")]
        public double TestLongMaxValueViaDouble { get; set; }

        [JsonProperty("createDate")]
        public DateTime CreateDate { get; set; }

        [JsonProperty("timeSpanTest")]
        public TimeSpan TimeSpanTest { get; set; }
    }
    public class TestParentClass
    {
        [JsonProperty("TestParent")]
        private string TestParent { get; set; } = "private parent property";
    }
}
