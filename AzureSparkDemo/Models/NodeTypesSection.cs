namespace AzureSparkDemo.Models
{
    public class NodeTypesSection : GenericConfigSection<NodeTypeExpression> { }

    /// <summary>
    /// Class expression.
    /// </summary>
    public class NodeTypeExpression
    {
        /// <summary>
        /// Gets or sets the value.
        /// </summary>
        /// <value>The value.</value>
        public string type { get; set; }

        /// <summary>
        /// Gets or sets the memory.
        /// </summary>
        /// <value>The memory.</value>
        public string memory { get; set; }

        /// <summary>
        /// Gets or sets the cost.
        /// </summary>
        /// <value>The cost.</value>
        public string cost { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"type={type}, memory={memory}, cost={cost}";
        }
    }
}
