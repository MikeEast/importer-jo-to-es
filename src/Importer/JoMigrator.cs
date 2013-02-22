using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore;
using EventStore.ClientAPI;
using Importer.Helpers;
using Importer.Infrastructure;

namespace Importer
{
    public class JoMigrator : IDisposable
    {
        private readonly string _joDbConnStringName;
        private readonly string _joDbConnString;
        private readonly IPEndPoint _esServerEndpoint;
        private readonly int _readPageSize;
        private readonly int _writePageSize;
        private readonly string _eventIdHeader;
        private readonly bool _checkpointsEnabled;

        private readonly Dictionary<string, List<EventData>> _batches = new Dictionary<string, List<EventData>>();
        private readonly Dictionary<string, Task> _tasks = new Dictionary<string, Task>();

        private readonly Stopwatch _sw = new Stopwatch();
        private readonly Stopwatch _gsw = new Stopwatch();

        private readonly int _maxBatchesCount;
        private readonly int _maxAttemptsForOperation;

        private IStoreEvents _joStore;
        private EventStoreConnection _eventStore;

        private ICheckpoint _checkpoint;

        private long _eventsToWrite;
        private long _eventsWritten;
        private long _eventsRead;

        private long _eventsReadInLastRun;
        private long _eventsWrittenInLastRun;

        private long _eventsReadAfterLastRun;
        private long _eventsWrittenAfterLastRun;

        private long _commitsRead;
        private readonly string _encryptKeyFilePath;

        private bool _failedAtWritingEvents;

        public JoMigrator(IPEndPoint esServerEndpoint, string joDbConnStringName, string eventIdHeader, int readPageSize, int writePageSize, int maxBatchesCount, int maxAttemptsForOperation, string encryptKeyFilePathFilePath)
        {
            _encryptKeyFilePath = encryptKeyFilePathFilePath;
            _eventsWritten = 0;
            _esServerEndpoint = esServerEndpoint;
            _joDbConnStringName = joDbConnStringName;
            _eventIdHeader = eventIdHeader;
            _readPageSize = readPageSize;
            _writePageSize = writePageSize;
            _maxBatchesCount = maxBatchesCount;
            _maxAttemptsForOperation = maxAttemptsForOperation;

            _joDbConnString = ConfigurationManager.ConnectionStrings[_joDbConnStringName].ConnectionString;
            _checkpointsEnabled = !string.IsNullOrEmpty(eventIdHeader);
        }

        public void StartMigration()
        {
            _gsw.Start();

            if (!string.IsNullOrEmpty(_encryptKeyFilePath))
            {
                byte[] encryptKey = GetEncryptKey();
                _joStore = JoEventStore.WireupEventStore(_readPageSize, _joDbConnStringName, encryptKey);
            }
            else
            {
                _joStore = JoEventStore.WireupEventStore(_readPageSize, _joDbConnStringName);
            }

            var esSettings = ConnectionSettings.Create()
                                               .LimitReconnectionsTo(0)
                                               .LimitAttemptsForOperationTo(_maxAttemptsForOperation)
                                               .LimitConcurrentOperationsTo(20);

            _eventStore = EventStoreConnection.Create(esSettings);

            _checkpoint = _checkpointsEnabled
                              ? (ICheckpoint)new Checkpoint(_esServerEndpoint.ToString(), _joDbConnString)
                              : new NullCheckpoint();


            var retryIterator = new RetryIterator(timeStamp => _joStore.Advanced.GetFrom(timeStamp));
            PageIterator pageIterator;

            PrepareForIteration(_checkpoint, out pageIterator, out _commitsRead);

            _sw.Restart();

            pageIterator.IterateByPage(_readPageSize,
                                       retryIterator.GetCommitsSince,
                                       processCommit: commit =>
                                       {
                                           if (_failedAtWritingEvents)
                                               throw new Exception("Failed at writing events to Event Store");

                                           ProcessCommit(commit);
                                           UpdateProgress();
                                       },
                                       onPageRead: (lastCommitDate, processedCommits) =>
                                       {
                                           LogProgress();
                                           FlushBatches(); //1. keep read/write window in normal range 2. prepare to checkpoint

                                           if (_failedAtWritingEvents)
                                               throw new Exception("Failed at writing events to Event Store");

                                           _checkpoint.Save(lastCommitDate, processedCommits, _commitsRead);
                                           _sw.Restart();
                                       });

            LogHelper.Log("Reads completed. Waiting for writes to complete...");

            FinishMigration();

            LogHelper.Log("Total processed {0} commits in {1} ({2}/s).",
                _commitsRead,
                _gsw.Elapsed,
                1000 * _commitsRead / _gsw.ElapsedMilliseconds);
        }

        private byte[] GetEncryptKey()
        {
            byte[] encryptKey;
            using (var reader = new StreamReader(_encryptKeyFilePath))
            {
                try
                {
                    encryptKey = reader.CurrentEncoding.GetBytes(reader.ReadToEnd());
                    if (encryptKey.Length != 16)
                        throw new Exception();
                }
                catch (Exception)
                {
                    throw new FileLoadException();
                }
            }
            return encryptKey;
        }

        private static void PrepareForIteration(ICheckpoint checkpoint, out PageIterator iterator, out long commitsRead)
        {
            Queue<Guid> processedQ;
            HashSet<Guid> processedS;
            DateTime lastProcessedCommit;

            LogHelper.Log("Checking for available checkpoint...");
            checkpoint.Load(out lastProcessedCommit, out processedQ, out processedS, out commitsRead);

            LogHelper.Log("Last Commit: {0}", lastProcessedCommit != DateTime.MinValue ? lastProcessedCommit.ToString("O") : "no previous commits");
            LogHelper.Log("Commits Read: {0}", commitsRead);

            iterator = new PageIterator(processedQ, processedS, lastProcessedCommit);
        }

        private void FinishMigration()
        {
            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    _sw.Restart();
                    Thread.Sleep(1000);
                    LogProgress();
                }
            });

            FlushBatches();

            _checkpoint.CleanUp();

            if (_failedAtWritingEvents)
                throw new Exception("Failed at writing events to Event Store ( after all tasks completed )");
        }

        private void ProcessCommit(Commit commit)
        {
            var streamId = commit.StreamId.ToString();
            var esEvents = commit.Events.Select(e => JoToEsEvent(e, _eventIdHeader));

            if (!_tasks.ContainsKey(streamId))
            {
                if (_tasks.Keys.Count > _maxBatchesCount)
                    FlushBatches();

                _tasks[streamId] = _eventStore.CreateStreamAsync(streamId, Guid.NewGuid(), true,
                                                                 commit.Headers.ToJsonMetadata());
            }

            List<EventData> streamEventsBatch;
            if (!_batches.TryGetValue(streamId, out streamEventsBatch))
            {
                streamEventsBatch = new List<EventData>();
                _batches[streamId] = streamEventsBatch;
            }
            streamEventsBatch.AddRange(esEvents);

            Interlocked.Add(ref _eventsToWrite, commit.Events.Count);
            Interlocked.Add(ref _eventsRead, commit.Events.Count);

            if (streamEventsBatch.Count > _writePageSize)
            {
                _batches.Remove(streamId);
                ProcessBatch(streamId, streamEventsBatch);
            }
        }

        private void ProcessBatch(string streamId, List<EventData> events)
        {
            _tasks[streamId] = _tasks[streamId].ContinueWith(task =>
            {
                _eventStore.AppendToStream(streamId,
                                           ExpectedVersion.Any,  // we could calculate version, but then checkpoints wouldn't work   
                                           events);
                if (!FinishTask(task))
                    return;

                Interlocked.Add(ref _eventsToWrite, -events.Count);
                Interlocked.Add(ref _eventsWritten, events.Count);
            });
        }

        private void FlushBatches()
        {
            foreach (var task in _batches)
                ProcessBatch(task.Key, task.Value);

            Task.WaitAll(_tasks.Values.ToArray());

            _batches.Clear();
            _tasks.Clear();
        }

        private bool FinishTask(Task task)
        {
            bool gracefulComplete = true;
            if (task.Exception != null)
            {
                LogHelper.Log("Exception: {0}", task.Exception);
                _failedAtWritingEvents = true;
                gracefulComplete = false;
            }

            task.Dispose();
            return gracefulComplete;
        }

        private void UpdateProgress()
        {
            _commitsRead += 1;

            if (_commitsRead % (_readPageSize / 10) == 0)
                Console.Write('.');
        }

        private void LogProgress()
        {
            var eventsWritten = Interlocked.Read(ref _eventsWritten);
            var eventsRead = Interlocked.Read(ref _eventsRead);
            var eventsToWrite = Interlocked.Read(ref _eventsToWrite);
            var elapsedMs = _sw.ElapsedMilliseconds;

            _eventsWrittenInLastRun = eventsWritten - _eventsWrittenAfterLastRun;
            _eventsReadInLastRun = eventsRead - _eventsReadAfterLastRun;

            _eventsWrittenAfterLastRun = eventsWritten;
            _eventsReadAfterLastRun = eventsRead;

            LogHelper.Log();
            LogHelper.Log("Total Commits Read: {0:0,0}, with speed: {1:0,0} commits/s).",
                _commitsRead,
                1000 * _readPageSize / elapsedMs);

            LogHelper.Log();
            LogHelper.Log("Events Just Read: {0:0,0}, with Speed: {1:0,0} ev/s. ",
                _eventsReadInLastRun,
                1000 * _eventsReadInLastRun / elapsedMs);

            LogHelper.Log("Events Just Written {0:0,0}, with Speed: {1:0,0} ev/s. ",
                _eventsWrittenInLastRun,
                1000 * _eventsWrittenInLastRun / elapsedMs);

            LogHelper.Log("Events Written Total: {0:0,0}", eventsWritten);
            LogHelper.Log("Events To Write: {0:0,0}", eventsToWrite);
            LogHelper.Log("Streams Count: {0}, ", _batches.Keys.Count);
            LogHelper.Log("Average Stream Events Count: {0}",
                          _batches.Values.Count == 0
                              ? 0
                              : _batches.Values.Sum(l => l.Count) / _batches.Values.Count);
            LogHelper.Log("Overall Time In Migration: {0}", _gsw.Elapsed.ToString("c"));
        }

        private static EventData JoToEsEvent(EventMessage joEvent, string eventIdHeader)
        {
            object type;
            if (!joEvent.Headers.TryGetValue(EsExportJsonSerializer.ImportTypeStr, out type))
                type = "jo-es-import-unknown-event-type";

            var typeStr = type.ToString();
            joEvent.Headers.Remove(EsExportJsonSerializer.ImportTypeStr); // it was added here on deserialization

            Guid eventIdGuid;

            if (string.IsNullOrEmpty(eventIdHeader))
                eventIdGuid = Guid.NewGuid();
            else
            {
                object eventIdObj;
                if (!joEvent.Headers.TryGetValue(eventIdHeader, out eventIdObj) ||
                    !Guid.TryParse(eventIdObj.ToString(), out eventIdGuid))
                {
                    throw new Exception("Couldn't get eventId from event headers. Terminating application in order to avoid duplicate event writes in case of failover.");
                }
            }

            var data = Encoding.UTF8.GetBytes((string) joEvent.Body);

            return new EventData(eventIdGuid, typeStr, true, data, joEvent.Headers.ToJsonMetadata());
        }

        public void Dispose()
        {
            if (_joStore != null)
                _joStore.Dispose();

            if (_eventStore != null)
                _eventStore.Close();
        }
    }
}