namespace KenshoDemo.Models
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Class PropertyComparable.
    /// </summary>
    [Serializable]
    public class PropertyComparable
    {
        /// <summary>
        /// Objects the equals.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public static bool ObjectEquals(object left, object right)
        {
            if (object.ReferenceEquals(left, null))
            {
                return object.ReferenceEquals(right, null);
            }
            else
            {

                if (left is ISet<string>)
                {
                    return (left as ISet<string>).SetEquals(right as ISet<string>);
                }

                if (left is IEnumerable<string>)
                {
                    return Enumerable.SequenceEqual(left as IEnumerable<string>, right as IEnumerable<string>);
                }

                if (left is IEnumerable<Guid>)
                {
                    return Enumerable.SequenceEqual(left as IEnumerable<Guid>, right as IEnumerable<Guid>);
                }

                if (left is IEnumerable<PropertyComparable>)
                {
                    return Enumerable.SequenceEqual(left as IEnumerable<PropertyComparable>, right as IEnumerable<PropertyComparable>);
                }

                if (left is IDictionary)
                {
                    var leftAsDict = GetEntries((IDictionary)left).ToDictionary(dictEntry => dictEntry.Key, dictEntry => dictEntry.Value);
                    var rightAsDict = GetEntries((IDictionary)right).ToDictionary(dictEntry => dictEntry.Key, dictEntry => dictEntry.Value);

                    return leftAsDict.Keys.Count == rightAsDict.Keys.Count && leftAsDict.Keys.All(x =>
                               rightAsDict.ContainsKey(x) && leftAsDict[x].Equals(rightAsDict[x]));
                }

                if (left is JToken)
                {
                    return left.ToString() == right.ToString();
                }

                return left.Equals(right);
            }
        }

        private static IEnumerable<DictionaryEntry> GetEntries(IDictionary uncastedDictionary)
        {
            foreach (DictionaryEntry dictionaryEntry in uncastedDictionary)
            {
                yield return dictionaryEntry;
            }
        }

        /// <summary>
        /// Determines whether the specified <see cref="object" /> is equal to this instance.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns><c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            // If parameter is null, return false.
            if (object.ReferenceEquals(obj, null))
            {
                return false;
            }

            // Optimization for a common success case.
            if (object.ReferenceEquals(this, obj))
            {
                return true;
            }

            // If run-time types are not exactly the same, return false.
            if (this.GetType() != obj.GetType())
            {
                return false;
            }

            var properties = this.GetProperties();

            foreach (var property in properties)
            {
                var thisValue = property.GetValue(this);
                var otherValue = property.GetValue(obj);
                if (!ObjectEquals(thisValue, otherValue))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.</returns>
        public override int GetHashCode()
        {
            var properties = this.GetProperties();
            int code = 0;
            foreach (var property in properties)
            {
                var value = property.GetValue(this);
                if (!object.ReferenceEquals(value, null))
                {
                    code ^= value.GetHashCode();
                }
            }

            return code;
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        /// <summary>
        /// Gets the properties.
        /// </summary>
        /// <returns>PropertyInfo[].</returns>
        private PropertyInfo[] GetProperties()
        {
            return this.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty);
        }
    }
}
