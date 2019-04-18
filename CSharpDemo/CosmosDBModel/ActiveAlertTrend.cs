using Newtonsoft.Json;
using System;

namespace CSharpDemo.CosmosDBModel
{
    public class ActiveAlertTrend
    {
        [JsonProperty(PropertyName = "id")]
        public string Id
        {
            get; set;
        }
        [JsonProperty(PropertyName = "timeStamp")]
        public DateTime TimeStamp
        {
            get; set;
        }
        [JsonProperty(PropertyName = "activeAlertCount")]
        public long ActiveAlertCount
        {
            get; set;
        }
    }
}
