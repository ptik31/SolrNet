using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SolrNet.Cloud  {
    /// <summary>
    /// Solr cloud operations base
    /// </summary>
    public abstract class SolrCloudOperationsBase<T> {
        /// <summary>
        /// Is post connection
        /// </summary>
        private readonly bool isPostConnection;

        /// <summary>
        /// Collection name
        /// </summary>
        private readonly string collectionName;

        /// <summary>
        /// Cloud state provider
        /// </summary>
        private readonly ISolrCloudStateProvider cloudStateProvider;

        /// <summary>
        /// Operations provider
        /// </summary>
        private readonly ISolrOperationsProvider operationsProvider;

        /// <summary>
        /// Random instance
        /// </summary>
        private readonly Random random;

        /// <summary>
        /// Constructor
        /// </summary>
        protected SolrCloudOperationsBase(ISolrCloudStateProvider cloudStateProvider, ISolrOperationsProvider operationsProvider, bool isPostConnection) {
            this.cloudStateProvider = cloudStateProvider;
            this.operationsProvider = operationsProvider;
            this.isPostConnection = isPostConnection;
            random = new Random();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        protected SolrCloudOperationsBase(ISolrCloudStateProvider cloudStateProvider, ISolrOperationsProvider operationsProvider, bool isPostConnection, string collectionName = null)
            : this(cloudStateProvider, operationsProvider, isPostConnection)
        {
            this.collectionName = collectionName;
        }

        /// <summary>
        /// Performs basic operation
        /// </summary>
        protected TResult PerformBasicOperation<TResult>(Func<ISolrBasicOperations<T>, TResult> operation, bool leader = false) {
            var replicas = SelectReplicas(leader);
            var pullReplicas = replicas.Where(replica => replica.ReplicaType == ReplicaType.PULL).ToList();
            var url = pullReplicas.Any()
                          ? pullReplicas[random.Next(pullReplicas.Count)].Url
                          : replicas[random.Next(replicas.Count)].Url;

            var operations = operationsProvider.GetBasicOperations<T>(url, isPostConnection);
            if (operations == null) throw new ApplicationException("Operations provider returned null.");

            return operation(operations);
        }

        /// <summary>
        /// Perform operation
        /// </summary>
        protected TResult PerformOperation<TResult>(Func<ISolrOperations<T>, TResult> operation, bool leader = false)
        {
            var replicas = SelectReplicas(leader);
            var pullReplicas = replicas.Where(replica => replica.ReplicaType == ReplicaType.PULL).ToList();
            var url = pullReplicas.Any()
                          ? pullReplicas[random.Next(pullReplicas.Count)].Url
                          : replicas[random.Next(replicas.Count)].Url;

            var operations = operationsProvider.GetOperations<T>(url, isPostConnection);
            if (operations == null) throw new ApplicationException("Operations provider returned null.");

            return operation(operations);
        }

        /// <summary>
        /// Returns collection of replicas
        /// </summary>
        private IList<SolrCloudReplica> SelectReplicas(bool leaders)
        {
            return ReloadCloudStateWithRetryAsync(async _ =>
                   {
                       try
                       {
                           var state = cloudStateProvider.GetCloudState();
                           if (state == null || state.Collections == null || state.Collections.Count == 0)
                           {
                               throw new ApplicationException("Didn't get any collection's state from zookeeper.");
                           }

                           if (collectionName != null && !state.Collections.ContainsKey(collectionName))
                           {
                               throw new ApplicationException(
                                   string.Format("Didn't get '{0}' collection state from zookeeper.", collectionName));
                           }

                           var collection = collectionName == null
                                                ? state.Collections.Values.First()
                                                : state.Collections[collectionName];
                           var replicas = collection.Shards.Values
                                                    .Where(shard => !leaders || shard.IsActive)
                                                    .SelectMany(shard => shard.Replicas.Values)
                                                    .Where(replica => replica.IsActive && (!leaders || replica.IsLeader))
                                                    .ToList();
                           if (replicas.Count == 0)
                           {
                               throw new ApplicationException("No appropriate node was selected to perform the operation.");
                           }
                           return replicas;
                       }
                       catch (ApplicationException)
                       {
                           await cloudStateProvider.GetFreshCloudStateAsync().ConfigureAwait(continueOnCapturedContext: false);
                           throw;
                       }
                   })
                   .GetAwaiter()
                   .GetResult();
        }

        private static async Task<TRetry> ReloadCloudStateWithRetryAsync<TRetry>(
            Func<CancellationToken, Task<TRetry>> operation,
            int timeoutPerTryMs = int.MaxValue,
            int maxRetries = 10,
            CancellationToken cancellationToken = default)
        {
            var timeout = TimeSpan.FromMilliseconds(timeoutPerTryMs);
            for (var attempt = 1; attempt <= maxRetries; attempt++)
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(timeout);

                try
                {
                    var operationTask = operation.Invoke(cts.Token);
                    var timeoutTask = Task.Delay(timeout, cts.Token);

                    await Task.WhenAny(operationTask, timeoutTask).ConfigureAwait(continueOnCapturedContext: false);

                    return await operationTask.ConfigureAwait(continueOnCapturedContext: false);
                }
                catch (Exception) when (attempt < maxRetries && !cancellationToken.IsCancellationRequested)
                {
                    var delay = GetExponentialBackoffDelay(attempt);
                    await Task.Delay(delay, cts.Token).ConfigureAwait(continueOnCapturedContext: false);

                    static TimeSpan GetExponentialBackoffDelay(int attempt)
                    {
                        const int baseDelayMs = 100;
                        var delayMs = baseDelayMs * (int)Math.Pow(x: 2, y: attempt - 1);
                        return TimeSpan.FromMilliseconds(delayMs);
                    }
                }
            }

            throw new TimeoutException(message: $"Could not reconnect to Solr Cluster (ZooKeeper) after {maxRetries} retries.");
        }
    }
}
