//<auto-generated />

using System.Runtime.Serialization;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Avro
{
    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.webserver.HeaderEntry.
    /// </summary>
    [Private]
    [DataContract(Namespace = "org.apache.reef.webserver")]
    public class HeaderEntry
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.webserver.HeaderEntry"",""fields"":[{""name"":""key"",""type"":""string""},{""name"":""value"",""type"":""string""}]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }
      
        /// <summary>
        /// Gets or sets the key field.
        /// </summary>
        [DataMember]
        public string key { get; set; }
              
        /// <summary>
        /// Gets or sets the value field.
        /// </summary>
        [DataMember]
        public string value { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="HeaderEntry"/> class.
        /// </summary>
        public HeaderEntry()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HeaderEntry"/> class.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public HeaderEntry(string key, string value)
        {
            this.key = key;
            this.value = value;
        }
    }
}