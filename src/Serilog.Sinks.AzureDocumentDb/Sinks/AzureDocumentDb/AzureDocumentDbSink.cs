// Copyright 2016 Serilog Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.AzureDocumentDB.Sinks.AzureDocumentDb;
using Serilog.Sinks.AzureDocumentDB.Sinks.Helpers;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.AzureDocumentDb
{
    internal class AzureDocumentDBSink : BatchProvider, ILogEventSink
    {
        private readonly CosmosClient _client;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly int? _timeToLive;
        private Container _container;
        private Database _database;
        private readonly SemaphoreSlim _semaphoreSlim;
        private readonly LogOptions _logOptions;
        private readonly string _partitionKey;

        public AzureDocumentDBSink(
            Uri endpointUri,
            string authorizationKey,
            string databaseName,
            string collectionName,
            string partitionKey,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            Microsoft.Azure.Cosmos.ConnectionMode connectionProtocol,
            TimeSpan? timeToLive,
            int logBufferSize = 25_000,
            int batchSize = 100,
            LogOptions logOptions = null 
            ) : base(batchSize, logBufferSize)
        {
            _formatProvider   = formatProvider;

            _partitionKey = partitionKey; // "/Properties/AuditType"

            if ((timeToLive != null) && (timeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int) timeToLive.Value.TotalSeconds;

            _client = new CosmosClient(
                endpointUri.ToString(), 
                authorizationKey,
                new CosmosClientOptions()
                {
                    AllowBulkExecution = true,
                    ConnectionMode = connectionProtocol,
                    GatewayModeMaxConnectionLimit = Environment.ProcessorCount * 50 + 200
                });

            _storeTimestampInUtc = storeTimestampInUtc;
            _semaphoreSlim       = new SemaphoreSlim(1, 1);

            CreateDatabaseIfNotExistsAsync(databaseName).Wait();
            CreateCollectionIfNotExistsAsync(collectionName).Wait();

            JsonConvert.DefaultSettings = () =>
                                          {
                                              var settings = new JsonSerializerSettings()
                                              {
                                                  ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                                                  ContractResolver      = new DefaultContractResolver()
                                              };

                                              return settings;
                                          };
            _logOptions = logOptions ?? new LogOptions();
        }

        private async Task CreateDatabaseIfNotExistsAsync(string databaseName)
        {
            SelfLog.WriteLine($"Opening database {databaseName}");
            _database = await _client.CreateDatabaseIfNotExistsAsync(databaseName);
        }

        private async Task CreateCollectionIfNotExistsAsync(string collectionName)
        {
            SelfLog.WriteLine($"Creating collection: {collectionName}");
            var containerProperties = new ContainerProperties()
            {
                Id = collectionName,
                PartitionKeyPath = _partitionKey,
                DefaultTimeToLive = _timeToLive
            };
            _container = await _database.CreateContainerIfNotExistsAsync(containerProperties);
        }

        #region Parallel Log Processing Support

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if (logEventsBatch == null || logEventsBatch.Count == 0)
                return true;

            if (_logOptions != null && _logOptions.IncludedProperties.Any())
            {
                foreach (var logEvent in logEventsBatch)
                {
                    foreach (var logEventProperty in logEvent.Properties)
                    {
                        if (!_logOptions.IncludedProperties.Contains(logEventProperty.Key))
                        {
                            logEvent.RemovePropertyIfPresent(logEventProperty.Key);
                        }
                    }
                }
            }

            var args = logEventsBatch.Select(x => x.Dictionary(_storeTimestampInUtc, _formatProvider));

            if (_logOptions != null && _logOptions.ExcludedStandardLogElements.Any())
            {
                args = args.Select(x =>
                {
                    x["id"] = Guid.NewGuid();
                    foreach (var logOptionsExcludedLogElement in _logOptions.ExcludedStandardLogElements)
                    {
                        x.Remove(LogOptions.MapStandardLogToKey(logOptionsExcludedLogElement));
                    }

                    return x;
                });
            }

            if ((_timeToLive != null) && (_timeToLive > 0))
                args = args.Select(
                    x =>
                    {
                        if (!x.Keys.Contains("ttl"))
                            x.Add("ttl", _timeToLive);

                        return x;
                    });
            await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                SelfLog.WriteLine($"Sending batch of {logEventsBatch.Count} messages to DocumentDB");

                await BulkMeUp(args);

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
            finally {
                _semaphoreSlim.Release();
            }
        }

        #endregion

        private async Task BulkMeUp(IEnumerable<IDictionary<string, object>> args)
        {
            BulkOperations<IDictionary<string, object>> bulkOperations = new BulkOperations<IDictionary<string, object>>(args.Count());

            foreach (IDictionary<string, object> document in args)
            {
                var paths = _partitionKey.Split('/').ToList();
                paths.RemoveAt(0);

                var partitionValue = GetPartitionValue(document, paths);
                bulkOperations.Tasks.Add(_container
                    .CreateItemAsync(document, new PartitionKey(partitionValue.ToString()))
                    .CaptureOperationResponse(document));
            }

            BulkOperationResponse<IDictionary<string, object>> bulkOperationResponse = await bulkOperations.ExecuteAsync();

            SelfLog.WriteLine($"Bulk create operation finished in {bulkOperationResponse.TotalTimeTaken}");
            SelfLog.WriteLine($"Consumed {bulkOperationResponse.TotalRequestUnitsConsumed} RUs in total");
            SelfLog.WriteLine($"Created {bulkOperationResponse.SuccessfulDocuments} documents");
            SelfLog.WriteLine($"Failed {bulkOperationResponse.Failures.Count} documents");
        }

        private object GetPartitionValue(IDictionary<string, object> dict, List<string> paths)
        {
            if (paths.Count == 0)
            {
                return null;
            }
            if (dict.TryGetValue(paths.First(), out var result))
            {
                if (paths.Count == 1)
                {
                    return result;
                }

                if (result is IDictionary<string, object> nextDict)
                {
                    var nextPaths = paths.Where((x, i) => i != 0).ToList();
                    return GetPartitionValue(nextDict, nextPaths);
                }
            }
            throw new ArgumentException();
        }

        #region ILogEventSink Support

        public void Emit(LogEvent logEvent)
        {
            if (logEvent != null) {
                PushEvent(logEvent);
            }
        }

        #endregion
    }
}
