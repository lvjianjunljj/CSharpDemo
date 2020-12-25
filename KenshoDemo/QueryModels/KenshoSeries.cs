
namespace KenshoDemo.QueryModels
{
    using System.Collections.Generic;
    using Newtonsoft.Json;

    /// <summary>
    /// This identifies a series given a set of dimensions.
    /// </summary>
    public class KenshoSeries
    {
        /// <summary>
        /// This is a list of dimensions and values that identifies a series.
        /// </summary>
        [JsonProperty(PropertyName = "dimensions")]
        public Dictionary<string, string> Dimensions { get; set; }
    }
}
