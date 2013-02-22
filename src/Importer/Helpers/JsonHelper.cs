using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace Importer.Helpers
{
    public static class JsonHelper
    {
        private static readonly JsonSerializerSettings _jsonSets = new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            NullValueHandling = NullValueHandling.Include,
            DefaultValueHandling = DefaultValueHandling.Include,
            MissingMemberHandling = MissingMemberHandling.Ignore,
            TypeNameHandling = TypeNameHandling.None,
            Converters = new JsonConverter[] { new StringEnumConverter() }
        };

        public static byte[] ToJsonMetadata(this Dictionary<string, object> dict)
        {
            if (dict == null)
                return null;

            var json = JsonConvert.SerializeObject(dict, Formatting.None, _jsonSets);
            var bytes = Encoding.UTF8.GetBytes(json);
            return bytes;
        }
    }
}