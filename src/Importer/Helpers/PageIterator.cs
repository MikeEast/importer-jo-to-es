using System;
using System.Collections.Generic;
using EventStore;

namespace Importer.Helpers
{
    public class PageIterator
    {
        private readonly Queue<Guid> _processedQ;
        private readonly HashSet<Guid> _processedS;
        private DateTime _lastProcessedCommit;

        public PageIterator(Queue<Guid> processedQ, HashSet<Guid> processedS, DateTime lastProcessedCommit)
        {
            _processedQ = processedQ;
            _processedS = processedS;
            _lastProcessedCommit = lastProcessedCommit;
        }


        public void IterateByPage(int pageSize,
                                  Func<DateTime, IEnumerable<Commit>> getCommitsSince,
                                  Action<Commit> processCommit,
                                  Action<DateTime, Queue<Guid>> onPageRead)
        {


            var cur = 0;
            bool gotSome = true;
            while (gotSome)
            {
                gotSome = false;

                foreach (var commit in getCommitsSince(_lastProcessedCommit))
                {
                    if (_processedS.Contains(commit.CommitId))
                    {
                        Console.Write("#");
                        continue;
                    }
                    if (_processedQ.Count >= pageSize * 2)
                    {
                        var id = _processedQ.Dequeue();
                        _processedS.Remove(id);
                    }
                    _processedQ.Enqueue(commit.CommitId);
                    _processedS.Add(commit.CommitId);

                    _lastProcessedCommit = commit.CommitStamp;
                    gotSome = true;

                    //---------
                    processCommit(commit);
                    //--------

                    cur++;
                    if (cur == pageSize)
                    {
                        cur = 0;
                        // Thread.Sleep(100);
                        break;
                    }
                }

                onPageRead(_lastProcessedCommit, _processedQ);
            }
        }
    }
}
