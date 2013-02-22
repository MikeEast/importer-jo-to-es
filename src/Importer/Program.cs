using System;
using System.Configuration;
using System.IO;
using System.Net;
using Importer.Helpers;
using Importer.Infrastructure.CommandLine;

namespace Importer
{
    class Program
    {
        public static void Main(string[] args)
        {
            JoImportOptions opts;
            if (!TryParseOptions(args, out opts))
                return;

            var esEndpoint = new IPEndPoint(opts.EsIp, opts.EsTcpPort);
            var joImporter = new JoMigrator(esEndpoint,
                                            opts.JoConnStringName,
                                            opts.EventIdHeader,
                                            opts.ReadPageSize,
                                            opts.WritePageSize,
                                            opts.MaxParallelStreams,
                                            opts.MaxAttemptsForOperation,
                                            opts.EncryptKeyFilePath);

            RegisterUnhandledExceptionHandler();

            try
            {
                joImporter.StartMigration();
            }
            catch (FileLoadException)
            {
                LogHelper.Log("File with encrypt data key not found, contains bed data of acces denied");
            }
            catch (EventStore.Persistence.StorageUnavailableException ex)
            {
                LogHelper.Log("Couldn't connect to JOliver storage. Connection string: {0}.{1}Details: {2}",
                    ConfigurationManager.ConnectionStrings[opts.JoConnStringName],
                    Environment.NewLine,
                    ex.Message);
                return;
            }
            catch (EventStore.ClientAPI.Exceptions.CannotEstablishConnectionException ex)
            {
                LogHelper.Log("Couldn't connect to Event Store server at {0}.{1}Details: {2}",
                    esEndpoint.ToString(),
                    Environment.NewLine,
                    ex.Message);
                return;
            }
            catch (Exception ex)
            {
                LogHelper.Log("Migration FAILED!");
                LogHelper.Log("Error: {0}", ex);
                return;
            }
            finally
            {
                joImporter.Dispose();
            }

            LogHelper.Log("Successfully migrated!");
        }

        private static bool TryParseOptions(string[] args, out JoImportOptions opts)
        {
            opts = new JoImportOptions();
            if (!CommandLineParser.Default.ParseArguments(args, opts))
                return false;

            var joConnString = ConfigurationManager.ConnectionStrings[opts.JoConnStringName];
            if (joConnString == null || string.IsNullOrEmpty(joConnString.ConnectionString))
            {
                LogHelper.Log("Couldnt find connection string '{0}'", opts.JoConnStringName);
                return false;
            }

            LogHelper.Log("EsEvent Store IP: {0}", opts.EsIp);
            LogHelper.Log("EsEvent Store Port: {0}", opts.EsTcpPort);
            LogHelper.Log("JOliver Connection String Name: {0}", opts.JoConnStringName);
            LogHelper.Log("Read Page Size: {0}", opts.ReadPageSize);
            LogHelper.Log("Write Page Size: {0}", opts.WritePageSize);
            return true;
        }

        private static void RegisterUnhandledExceptionHandler()
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, exErgs) =>
            {
                LogHelper.Log("UNHANDLED EXCEPTION");
                LogHelper.Log("Is Terminating: {0}", exErgs.IsTerminating);
                LogHelper.Log("Exception: {0}", exErgs.ExceptionObject ?? "null");
            };
        }
    }
}