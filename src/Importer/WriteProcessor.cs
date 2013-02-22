using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using EventStore.ClientAPI;
using Importer.Helpers;

namespace Importer
{
    public class WriteProcessor
    {
        private readonly EventStoreConnection _connection;
        private readonly int _maxParallelQueues;
        private readonly int _maxMainQueueSize;
        private readonly int _maxStreamQueueSize;
        private readonly int _maxBatchSize;
        private readonly bool _optimistic;
        private readonly ConcurrentQueue<QueueItem> _mainQueue = new ConcurrentQueue<QueueItem>();
        private Thread _mainQueueThread;
        private Thread _subQueuesThread;
        private volatile bool _stop;
        private volatile bool _isValid = true;

        private readonly Dictionary<string, ConcurrentQueue<BatchQueueItem>> _subQueues;
        private readonly object _qLock = new object();

        private readonly Dictionary<string, bool> _inProgress = new Dictionary<string, bool>();
        private readonly object _inProgressLock = new object();

        private readonly Dictionary<string, BatchQueueItem> _batches = new Dictionary<string, BatchQueueItem>();

        public WriteProcessor(EventStoreConnection connection,
            int maxParallelQueues,
            int maxMainQueueSize,
            int maxStreamQueueSize,
            int maxBatchSize,
            bool optimistic)
        {
            _connection = connection;
            _maxParallelQueues = maxParallelQueues;
            _maxMainQueueSize = maxMainQueueSize;
            _maxStreamQueueSize = maxStreamQueueSize;
            _maxBatchSize = maxBatchSize;
            _optimistic = optimistic;
            _subQueues = new Dictionary<string, ConcurrentQueue<BatchQueueItem>>(maxParallelQueues);
        }

        public int QueueSize { get { return _mainQueue.Count; } }
        public int ActualParallelStreams
        {
            get
            {
                lock (_inProgressLock)
                {
                    return _inProgress.Keys.Count;
                }
            }
        }

        public void Start()
        {
            _mainQueueThread = new Thread(MainQueueLoop);
            _stop = false;
            _mainQueueThread.Start();

            _subQueuesThread = new Thread(SubQueuesLoop);
            _subQueuesThread.Start();
        }

        public void Stop()
        {
            _stop = true;
        }

        public void WaitForCompletion()
        {
            bool working = true;

            BatchQueueItem[] batches;
            lock (_batchLock)
            {
                batches = _batches.Values.ToArray();
            }

            lock (_qLock)
            {
                foreach (var batch in batches)
                    _subQueues[batch.StreamId].Enqueue(batch);
            }

            while (working)
            {
                lock (_qLock)
                    lock (_inProgressLock)
                    {
                        working = _mainQueue.Count > 0 || _subQueues.Any(kvp => kvp.Value.Count > 0) || _inProgress.Keys.Count > 0;

                        if (!_isValid)
                            throw new Exception("error while writing events to Event Store");
                    }
                Thread.Sleep(10);
            }
        }

        private readonly object _batchLock = new object();

        public bool IsValid()
        {
            return _isValid;
        }

        public void Enqueue(string streamId, IEnumerable<EventData> events, Action onProcessed)
        {
            while (_mainQueue.Count > _maxMainQueueSize)
                Thread.Sleep(1);

            _mainQueue.Enqueue(new QueueItem(streamId, events, onProcessed));
        }

        private void MainQueueLoop()
        {
            while (!_stop)
            {
                QueueItem item;
                if (_mainQueue.TryDequeue(out item))
                {
                    try
                    {
                        if (!_subQueues.ContainsKey(item.StreamId))
                        {
                            while (_subQueues.Count >= _maxParallelQueues)
                            {
                                Thread.Sleep(1);

                                lock (_qLock)
                                {
                                    var toRemove = new List<string>();
                                    foreach (var kvp in _subQueues)
                                    {
                                        if (kvp.Value.Count == 0)
                                            toRemove.Add(kvp.Key);
                                    }

                                    foreach (var streamId in toRemove)
                                    {
                                        _subQueues.Remove(streamId);
                                    }
                                }
                            }

                            lock (_qLock)
                            {
                                if (_subQueues.Count < _maxParallelQueues)
                                {
                                    _subQueues.Add(item.StreamId, new ConcurrentQueue<BatchQueueItem>());
                                }
                            }
                        }

                        var streamQueue = _subQueues[item.StreamId];

                        while (streamQueue.Count > _maxStreamQueueSize)
                            Thread.Sleep(1);

                        BatchQueueItem batch;
                        lock (_batchLock)
                        {
                            if (!_batches.ContainsKey(item.StreamId))
                                _batches.Add(item.StreamId, new BatchQueueItem(item.StreamId));

                            batch = _batches[item.StreamId];
                        }
                        if (!batch.AddItem(item, _maxBatchSize))
                        {
                            lock (_batchLock)
                                _batches.Remove(item.StreamId);

                            streamQueue.Enqueue(batch);
                        }
                    }
                    catch (Exception)
                    {
                        _isValid = false;
                    }
                }
                else
                {
                    Thread.Sleep(1);
                }
            }
        }

        private void SubQueuesLoop()
        {
            while (!_stop)
            {
                while (_subQueues.Count < 1 || _subQueues.Count > _maxParallelQueues)
                    Thread.Sleep(1);

                var freeQueues = new List<ConcurrentQueue<BatchQueueItem>>();

                lock (_qLock)
                    lock (_inProgressLock)
                    {
                        foreach (var kvp in _subQueues.Where(kvp => !_inProgress.ContainsKey(kvp.Key) && kvp.Value.Count > 0))
                        {
                            freeQueues.Add(kvp.Value);

                            if (!_optimistic)
                                _inProgress.Add(kvp.Key, true);
                        }
                    }

                if (freeQueues.Count < 1)
                {
                    Thread.Sleep(1);
                    continue;
                }

                foreach (var queue in freeQueues)
                {
                    BatchQueueItem item;
                    if (!queue.TryDequeue(out item))
                        continue;

                    if (!_isValid)
                        return;

                    LogHelper.Log("Sending batch with size {0} to stream {1}", item.Events.Count, item.StreamId);

                    _connection.AppendToStreamAsync(item.StreamId, ExpectedVersion.Any, item.Events)
                               .ContinueWith(t =>
                     {
                         lock (_inProgressLock)
                             _inProgress.Remove(item.StreamId);

                         if (t.Exception != null)
                         {
                             _isValid = false;
                             LogHelper.Log("Error when appending to stream {0}: {1}", item.StreamId, t.Exception);
                             return;

                         }

                         LogHelper.Log("Done sending batch with size {0} to stream {1}", item.Events.Count, item.StreamId);

                         var sw = Stopwatch.StartNew();

                         item.TriggerProcessed();

                         LogHelper.Log("TRIGGERING TOOK {0}ms", sw.ElapsedMilliseconds);
                     });
                }
            }
        }

        private class QueueItem
        {
            public readonly string StreamId;
            public readonly IEnumerable<EventData> Events;
            public readonly Action OnProcessed;

            public QueueItem(string streamId, IEnumerable<EventData> events, Action onProcessed)
            {
                StreamId = streamId;
                Events = events;
                OnProcessed = onProcessed;
            }
        }

        private class BatchQueueItem
        {
            public readonly string StreamId;
            public readonly List<EventData> Events;
            public readonly List<Action> OnProcessed;

            public BatchQueueItem(string streamId)
            {
                StreamId = streamId;
                Events = new List<EventData>();
                OnProcessed = new List<Action>();
            }

            public bool AddItem(QueueItem item, int maxSize)
            {
                Events.AddRange(item.Events);
                OnProcessed.Add(item.OnProcessed);

                return Events.Count < maxSize;
            }

            public void TriggerProcessed()
            {
                foreach (var action in OnProcessed)
                {
                    if (action != null)
                        action();
                }
            }
        }
    }
}
