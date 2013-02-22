using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using EventStore;

namespace Importer.Helpers
{
    public class RetryIterator
    {
        private readonly Func<DateTime, IEnumerable<Commit>> _originalEnumerator;

        public RetryIterator(Func<DateTime, IEnumerable<Commit>> originalEnumerator)
        {
            _originalEnumerator = originalEnumerator;
        }

        public IEnumerable<Commit> GetCommitsSince(DateTime timeStamp)
        {
            var commits = _originalEnumerator(timeStamp);

            IEnumerator<Commit> enumerator = null;
            try
            {
                enumerator = commits.GetEnumerator();
                while (MoveNext(enumerator))
                {
                    var commit = GetCommit(enumerator);
                    yield return commit;
                }
            }
            finally
            {
                if (enumerator != null)
                {
                    Dispose(enumerator);// why this hangs inside try block??
                }
            }
        }

        private static bool MoveNext(IEnumerator enumerator)
        {
            var retriesLeft = 3;
            while (true)
            {
                try
                {
                    return enumerator.MoveNext();
                }
                catch (Exception)
                {
                    LogHelper.Log("error when moving to next commit in database...retries left: {0}", retriesLeft);

                    if (retriesLeft > 0)
                        retriesLeft--;
                    else
                        throw;

                    Thread.Sleep(500);
                }
            }

        }

        private static Commit GetCommit(IEnumerator<Commit> enumerator)
        {
            var retriesLeft = 3;
            while (true)
            {
                try
                {
                    return enumerator.Current;
                }
                catch (Exception)
                {
                    LogHelper.Log("error when reading commit from database...retries left: {0}", retriesLeft);

                    if (retriesLeft > 0)
                        retriesLeft--;
                    else
                        throw;

                    Thread.Sleep(500);
                }
            }
        }

        private static void Dispose(IDisposable enumerator)
        {
            var retriesLeft = 3;
            while (retriesLeft > 0)
            {
                try
                {
                    enumerator.Dispose();
                    return;
                }
                catch (Exception)
                {
                    retriesLeft--;
                    LogHelper.Log("error when disposing enumerator...retries left: {0}", retriesLeft);

                    Thread.Sleep(500);
                }
            }
            LogHelper.Log("couldnt dispose enumerator... leaving it undisposed");
        }
    }
}
