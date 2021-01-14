namespace CSharpDemo.InvalidCastExceptionDemo
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using System.Collections.Generic;

    class SubClass : BaseClass
    {
        /// <summary>
        /// Gets or sets the rolling window.
        /// </summary>
        public TimeSpan? RollingWindow { get; set; }

        /// <summary>
        /// The direct name, instead of friendly name as per <see cref="Name"/>
        /// </summary>
        public string RawName { get; set; }

        /// <summary>
        /// The version of the dataset this config represents
        /// </summary>
        public int Version { get; set; }

        /// <summary>
        /// Measures associated with this config
        /// </summary>
        public List<MeasureConfiguration> Measures;
    }

    /// <summary>
    /// The measure configuration.
    /// </summary>
    class MeasureConfiguration
    {
        /// <summary>
        /// Gets or sets the name of the measure definition
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the target (usually column name) of the measure
        /// </summary>
        public string Target { get; set; }

        /// <summary>
        /// Gets or sets the type of aggregate being performed
        /// </summary>
        public AggregateType Aggregate { get; set; }

        /// <summary>
        /// Gets or sets filters on the measure
        /// </summary>
        public List<string> Filters { get; set; }

        /// <summary>
        /// Gets or sets the set of dimension columns.
        /// </summary>
        public List<string> Dimensions { get; set; }

        /// <summary>
        /// The set of tests associated with this measure.
        /// </summary>
        public List<TestConfiguration> Tests { get; set; }

        /// <summary>
        /// The location of where this measure will be emitted. Set during execution
        /// </summary>
        public string MeasureLayout { get; set; }
    }

    /// <summary>
    /// Represents the type of aggregation to be performed.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    enum AggregateType
    {
        None = 0,

        Count = 1,

        DistinctCount = 2,

        PercentOf = 3,

        Mean = 4,

        StdDev = 5,

        Normal = 6,

        Size = 7,

        Custom = 8
    }

    class TestConfiguration
    {
        /// <summary>
        /// The name of the test, used for alerting and 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The test expression to be evaluated.
        /// </summary>
        public string Expression { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this test is considered gating.
        /// </summary>
        public bool IsGating { get; set; }
    }
}
