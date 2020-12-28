namespace KenshoDemo.Models
{
    using System;
    using System.Collections.Generic;
    using CommonLib;
    using Newtonsoft.Json;

    /// <summary>
    /// This is used to store Kensho metric related data in a dataset.
    /// </summary>
    public class KenshoMetric : PropertyComparable
    {
        /// <summary>
        /// This gets or sets the name of the metric.
        /// </summary>
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }

        /// <summary>
        /// This gets or sets the Kensho-assigned Id to the metric to configure.
        /// </summary>
        [JsonProperty(PropertyName = "metricGuid")]
        public Guid? MetricGuid { get; set; }

        /// <summary>
        /// This gets or sets the determination of whether an alert should be generated for this metric overall, or for each series instead.  We should
        /// probably always set this to false for now so we can generate separate alerts for each combination of dimensions.
        /// </summary>
        [JsonProperty(PropertyName = "enableMetricAlert")]
        public bool? EnableMetricAlert { get; set; }

        /// <summary>
        /// This gets or sets the name to assign to this configuration.
        /// </summary>
        [JsonProperty(PropertyName = "configName")]
        public string ConfigName { get; set; }

        /// <summary>
        /// This gets or sets the decription to assign to this configuration.
        /// </summary>
        [JsonProperty(PropertyName = "configDescription")]
        public string ConfigDescription { get; set; }

        /// <summary>
        /// This gets or sets the detector type, which can be 1=SmartDetection, 2=HardThreshold, 4=ChargeThreshold
        /// </summary>
        [JsonProperty(PropertyName = "anomalyDetectorType")]
        public string AnomalyDetectorType { get; set; }

        /// <summary>
        /// This gets or sets the detector direction, which can be 1=negative, 2=positive, 3=both
        /// </summary>
        [JsonProperty(PropertyName = "anomalyDetectorDirection")]
        public string AnomalyDetectorDirection { get; set; }

        /// <summary>
        /// This determines whether the anomalies will generate alerts as soon as detected, or if they will be delayed 
        /// to allow for other anomalies to be found.
        /// </summary>
        [JsonProperty(PropertyName = "autoSnooze")]
        public int? AutoSnooze { get; set; }

        /// <summary>
        /// This gets or sets the number of anomalies to be found before an alert is generated.
        /// </summary>
        [JsonProperty(PropertyName = "minAlertNumber")]
        public double? MinAlertNumber { get; set; }

        /// <summary>
        /// This gets or sets the ratio of anomalies foound over the past x data points before an alert is generated.
        /// </summary>
        [JsonProperty(PropertyName = "minAlertRatio")]
        public double? MinAlertRatio { get; set; }

        /// <summary>
        /// Gets or sets the minimum anomaly severity to look for when querying Kensho.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("minSeverity")]
        public int? MinSeverity { get; set; }

        /// <summary>
        /// Gets or sets the maximum anomaly severity to look for when querying Kensho.
        /// </summary>
        /// <value>Guid</value>
        [JsonProperty("maxSeverity")]
        public int? MaxSeverity { get; set; }

        /// <summary>
        /// This gets or sets the sensistivity (0-100) of detection.
        /// </summary>
        [JsonProperty(PropertyName = "sensitivity")]
        public double? Sensitivity { get; set; }

        /// <summary>
        /// This gets or sets the upperBound of detection, used for hard threshold detect.
        /// </summary>
        [JsonProperty(PropertyName = "upperBound")]
        public double? UpperBound { get; set; }

        /// <summary>
        /// This gets or sets the lowerBound of detection, used for hard threshold detect.
        /// </summary>
        [JsonProperty(PropertyName = "lowerBound")]
        public double? LowerBound { get; set; }

        /// <summary>
        /// This gets or sets the changePercentage of detection, used for change threshold detect.
        /// </summary>
        [JsonProperty(PropertyName = "changePercentage")]
        public double? ChangePercentage { get; set; }

        /// <summary>
        /// This gets or sets the changePercentageOver of detection, used for change threshold detect.
        /// </summary>
        [JsonProperty(PropertyName = "changePercentageOver")]
        public double? ChangePercentageOver { get; set; }

        /// <summary>
        /// This gets or sets thelist of hooks the metric should send alerts to.
        /// </summary>
        [JsonProperty(PropertyName = "hookList")]
        public List<Guid> HookList { get; set; }

        /// <summary>
        /// This gets or sets the Kensho-assigned Id to the detection configuration for this metric.
        /// </summary>
        [JsonProperty(PropertyName = "configId")]
        public Guid? ConfigId { get; set; }

        /// <summary>
        /// This gets or sets the fovorite series.
        /// </summary>
        [JsonProperty(PropertyName = "dimensionGroupOverrideConfigs")]
        public List<object> DimensionGroupOverrideConfigs { get; set; }
    }
}

