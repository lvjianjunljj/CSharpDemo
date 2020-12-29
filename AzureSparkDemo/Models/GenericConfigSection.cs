namespace AzureSparkDemo.Models
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Reflection;
    using System.Xml;

    /// <summary>
    /// Class DeleteProcessingRootRegexesSection.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <seealso cref="System.Configuration.IConfigurationSectionHandler" />
    public class GenericConfigSection<T> : IConfigurationSectionHandler
            where T : new()
    {
        /// <summary>
        /// Creates a configuration section handler.
        /// </summary>
        /// <param name="parent">Parent object.</param>
        /// <param name="configContext">Configuration context object.</param>
        /// <param name="section">Section XML node.</param>
        /// <returns>The created section handler object.</returns>
        public object Create(object parent, object configContext, XmlNode section)
        {
            var list = new List<T>();
            var properties = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.GetProperty | BindingFlags.SetProperty | BindingFlags.Public);
            foreach (XmlNode childNode in section.ChildNodes)
            {
                var element = new T();
                foreach (var property in properties)
                {
                    string value = childNode.Attributes.Cast<XmlAttribute>().Where(t => t.Name == property.Name).FirstOrDefault()?.Value;
                    property.SetValue(element, value);
                }
                list.Add(element);
            }
            return list;
        }
    }

}
