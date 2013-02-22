using EventStore;
using EventStore.Persistence.SqlPersistence.SqlDialects;

namespace Importer.Infrastructure
{
    public static class JoEventStore
    {
        public static IStoreEvents WireupEventStore(int pageSize, string connStringName)
        {
            return Wireup.Init()
                         .LogToOutputWindow()
                         .UsingSqlPersistence(connStringName) // Connection string is in app.config
                         .PageEvery(pageSize)
                         .WithDialect(new MsSqlDialect())
                         .InitializeStorageEngine()
                         .UsingCustomSerialization(new EsExportJsonSerializer())
                         .Build();
        }

        public static IStoreEvents WireupEventStore(int pageSize, string connStringName, byte[] encryptKey)
        {
            return Wireup.Init()
                         .UsingSqlPersistence(connStringName)
                         .PageEvery(pageSize)
                         .WithDialect(new MsSqlDialect())
                         .EnlistInAmbientTransaction()
                         .InitializeStorageEngine()
                         .UsingCustomSerialization(new EsExportJsonSerializer()).Compress().EncryptWith(encryptKey)
                         .Build();
        }
    }
}
