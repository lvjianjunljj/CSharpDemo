namespace KenshoDemo.QueryModels
{
    using KenshoDemo.Models;
    using System.Collections.Generic;

    /// <summary>
    /// This is the query_detect_result_anomalies response returned by Kensho.  
    /// </summary>
    public class KenshoQueryDetectResultAnomaliesResponse : KenshoBaseResponse
    {
        /// <summary>
        /// This gets or sets the anomaly set list.
        /// </summary>
        public List<KenshoDetectAnomaly> AnomalySetList { get; set; }
    }
}
