using System.Net;
using Importer.Infrastructure.CommandLine;

namespace Importer
{
    public class JoImportOptions : CommandLineOptionsBase
    {
        [Option(null, "es-ip", Required = true, HelpText = "An IP Adress of Event Store server")]
        public IPAddress EsIp { get; set; }

        [Option(null, "es-tcp-port", DefaultValue = 1113, HelpText = "A TCP Port of Event Store server (Default = 1113)")]
        public int EsTcpPort { get; set; }

        [Option(null, "jo-conn-string-name", Required = true, HelpText = "Name of connection string to JOliver storage. Currently only SQL storage is supported. Connection string has to be present in configuration file")]
        public string JoConnStringName { get; set; }

        [Option(null, "read-page-size", DefaultValue = 5000, HelpText = "A size of page to read data from JOliver storage (Default = 5000)")]
        public int ReadPageSize { get; set; }

        [Option(null, "write-page-size", DefaultValue = 500, HelpText = "A size of page to write data to Event Store (Default = 500)")]
        public int WritePageSize { get; set; }

        [Option(null, "max-parallel-streams", DefaultValue = 15, HelpText = "A maximal amount of parallel streams to process (Default = 15)")]
        public int MaxParallelStreams { get; set; }

        [Option(null, "max-attempts-for-operation", DefaultValue = 5, HelpText = "A maximal amount of retries if operation had failed(Default = 5)")]
        public int MaxAttemptsForOperation { get; set; }

        [Option(null, "event-id-header", HelpText = "String representing eventId key in EventMessage headers. If its not set, then checkpoint mechanism woulnd't work")]
        public string EventIdHeader { get; set; }

        [Option(null, "jo-encrypt-key-file-path", HelpText = "String representing file path to Jonathan Oliver Event Store encrypt key")]
        public string EncryptKeyFilePath { get; set; }

        [HelpOption]
        public virtual string GetUsage()
        {
            return HelpText.AutoBuild(this, (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}