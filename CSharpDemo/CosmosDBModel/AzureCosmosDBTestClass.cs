using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.CosmosDBModel
{
    public class AzureCosmosDBTestClass : TestParentClass
    {
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
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("timestampTicks")]
        public long TimestampTicks
        {
            get { return DateTime.UtcNow.Ticks; }
        }

    }
    public class TestParentClass
    {
        [JsonProperty("TestParent")]
        private string TestParent { get; set; } = "private parent property";
    }
}
