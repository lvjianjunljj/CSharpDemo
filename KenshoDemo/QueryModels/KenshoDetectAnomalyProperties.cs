namespace KenshoDemo.QueryModels
{
    /// <summary>
    /// The properties of an anomaly found by a Kensho detect config.
    /// </summary>
    public class KenshoDetectAnomalyProperties
    {
        /// <summary>
        /// This gets or sets the expected value of the datapoint when the anomaly occurred.
        /// </summary>
        public double? ExpectedValue { get; set; }

        /// <summary>
        /// This gets or sets the value of the severity of the anomaly.
        /// </summary>
        public string AnomalySeverity { get; set; }
    }
}
