using System.Data;
using Grpc.Core;
using Hydration.Grpc;
using MicroService_B.Data;
using Microsoft.EntityFrameworkCore;

namespace MicroserviceB.Services;

public class DataSyncProcessor
{
    private readonly DataHydration.DataHydrationClient _grpcClient;
    private readonly ServiceBDbContext _dbContext;
    private readonly ILogger<DataSyncProcessor> _logger;
    private const string LockResourceName = "BookingHydrationLock";

    public DataSyncProcessor(
        DataHydration.DataHydrationClient grpcClient,
        ServiceBDbContext dbContext,
        ILogger<DataSyncProcessor> logger)
    {
        _grpcClient = grpcClient;
        _dbContext = dbContext;
        _logger = logger;
    }

    public async Task RunHydrationPipelineAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Attempting to acquire distributed lock for Ingestion Pipeline...");

        // 1. Explicitly open connection to bind the session-level lock
        await _dbContext.Database.OpenConnectionAsync(cancellationToken);
        var connection = _dbContext.Database.GetDbConnection();

        // 2. Request a session-level exclusive application lock from SQL Server
        // @LockTimeout = 0 forces an immediate failure if another instance holds the lock
        using (var lockCommand = connection.CreateCommand())
        {
            lockCommand.CommandText = $@"
                DECLARE @lockResult INT;
                EXEC @lockResult = sp_getapplock 
                    @Resource = '{LockResourceName}', 
                    @LockMode = 'Exclusive', 
                    @LockOwner = 'Session', 
                    @LockTimeout = 0;
                SELECT @lockResult;";

            var result = await lockCommand.ExecuteScalarAsync(cancellationToken);
            int lockStatus = Convert.ToInt32(result);

            if (lockStatus < 0)
            {
                _logger.LogWarning("Distributed lock acquisition failed. Another microservice instance is actively running the sync.");
                throw new InvalidOperationException("The synchronization pipeline is locked and currently executing on another instance. Please retry later.");
            }
        }

        _logger.LogInformation("Distributed lock '{LockName}' acquired successfully. Starting Snapshot Isolation Ingestion...", LockResourceName);

        // 3. Start the SNAPSHOT transaction safely under the acquired lock
        await using var transaction = await _dbContext.Database.BeginTransactionAsync(
            IsolationLevel.Snapshot,
            cancellationToken
        );

        try
        {
            // Truncate previous data inside the transaction to simulate a fresh sync
            await _dbContext.Database.ExecuteSqlRawAsync("TRUNCATE TABLE SyncedBookings", cancellationToken);

            var streamRequest = new HydrationRequest { BatchSize = 500 };
            using var streamingCall = _grpcClient.StreamData(streamRequest, cancellationToken: cancellationToken);

            var memoryBatch = new List<SyncedBooking>();
            int processedCount = 0;

            // 4. Process the HTTP/2 stream asynchronously
            await foreach (var item in streamingCall.ResponseStream.ReadAllAsync(cancellationToken))
            {
                memoryBatch.Add(new SyncedBooking
                {
                    Id = Guid.Parse(item.Id),
                    SyncTimestamp = DateTime.UtcNow,
                    RawPayload = item.PayloadData
                });

                // Batch database writes every 500 records to maximize EF throughput
                if (memoryBatch.Count >= 500)
                {
                    await _dbContext.SyncedBookings.AddRangeAsync(memoryBatch, cancellationToken);
                    await _dbContext.SaveChangesAsync(cancellationToken);
                    processedCount += memoryBatch.Count;
                    memoryBatch.Clear();
                }
            }

            // Save remaining elements
            if (memoryBatch.Any())
            {
                await _dbContext.SyncedBookings.AddRangeAsync(memoryBatch, cancellationToken);
                await _dbContext.SaveChangesAsync(cancellationToken);
                processedCount += memoryBatch.Count;
            }

            // 5. Atomically commit the snapshot
            await transaction.CommitAsync(cancellationToken);
            _logger.LogInformation("Snapshot Ingestion successful! Committed {Count} rows cleanly.", processedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Pipeline failed. Rolling back snapshot transaction states.");
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
        finally
        {
            // 6. Guarantee that the distributed lock is released and connection is closed safely
            _logger.LogInformation("Releasing distributed lock '{LockName}'...", LockResourceName);
            using (var releaseCommand = connection.CreateCommand())
            {
                releaseCommand.CommandText = $"EXEC sp_releaseapplock @Resource = '{LockResourceName}', @LockOwner = 'Session';";
                await releaseCommand.ExecuteScalarAsync();
            }

            await _dbContext.Database.CloseConnectionAsync();
        }
    }
}