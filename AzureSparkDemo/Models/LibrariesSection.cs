namespace AzureSparkDemo.Models
{
    public class LibrariesSection : GenericConfigSection<LibraryExpression> { }

    public class LibraryExpression
    {

        public string path { get; set; }

        public string comment { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"path={path}, comment={comment}";
        }
    }
}
