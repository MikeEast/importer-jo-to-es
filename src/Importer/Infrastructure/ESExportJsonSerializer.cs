using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using EventStore;
using EventStore.Logging;
using EventStore.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Importer.Infrastructure
{
    public class EsExportJsonSerializer : ISerialize
    {
        public const string ImportTypeStr = "$$jo-es-migration-type-str$$";

        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(EsExportJsonSerializer));

        private static readonly JsonSerializer TypedSerializer = new JsonSerializer
        {
            TypeNameHandling = TypeNameHandling.All,
            DefaultValueHandling = DefaultValueHandling.Ignore,
            NullValueHandling = NullValueHandling.Ignore,
        };

        public virtual void Serialize<T>(Stream output, T graph)
        {
            Logger.Verbose("Serializing graph {0}", typeof(T));
            using (var streamWriter = new StreamWriter(output, Encoding.UTF8))
                Serialize(new JsonTextWriter(streamWriter), graph);
        }

        protected virtual void Serialize(JsonWriter writer, object graph)
        {
            using (writer)
                GetSerializer(graph.GetType()).Serialize(writer, graph);
        }

        public virtual T Deserialize<T>(Stream input)
        {
            if (typeof(T) == typeof(List<EventMessage>))
            {
                var result = new List<EventMessage>();
                using (var streamReader = new StreamReader(input, Encoding.UTF8))
                {
                    var json = JArray.Parse(streamReader.ReadToEnd());
                    foreach (var item in json)
                    {
                        var eventMessage = new EventMessage {Body = item["Body"].ToString()};
                        var type = item["Body"]["$type"];
                        if (type != null)
                        {
                            // just extra safeness
                            if (eventMessage.Headers.ContainsKey(ImportTypeStr))
                                type = eventMessage.Headers[ImportTypeStr].ToString();

                            eventMessage.Headers.Add(ImportTypeStr, type.ToString());
                        }

                        foreach (var headerItem in (JObject)item["Headers"])
                        {
                            eventMessage.Headers.Add(headerItem.Key, headerItem.Value.ToString());
                        }
                        result.Add(eventMessage);
                    }
                }
                return (T)Activator.CreateInstance(typeof(T), result);
            }

            Logger.Verbose("Deserializing stream for {0}", typeof(T));
            using (var streamReader = new StreamReader(input, Encoding.UTF8))
                return Deserialize<T>(new JsonTextReader(streamReader));
        }

        protected virtual T Deserialize<T>(JsonReader reader)
        {
            var type = typeof(T);

            using (reader)
                return (T)GetSerializer(type).Deserialize(reader, type);
        }

        protected virtual JsonSerializer GetSerializer(Type typeToSerialize)
        {
            return TypedSerializer;
        }
    }
}