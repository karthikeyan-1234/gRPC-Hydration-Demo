using Grpc.Core;
using Hydration.Grpc;
using MicroService_B.Data;
using Microsoft.EntityFrameworkCore;
using System.Data;

namespace MicroserviceB.Services;

public class DataSyncProcessor
{
    private readonly DataHydration.DataHydrationClient _grpcClient;
    private readonly ServiceBDbContext _dbContext;
    private readonly ILogger<DataSyncProcessor> _logger;

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
        _logger.LogInformation("Starting Snapshot Isolation Ingestion Pipeline...");

        // 1. Explicitly open connection to bind the transaction scope
        await _dbContext.Database.OpenConnectionAsync(cancellationToken);

        // 2. Start the SNAPSHOT transaction
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

            // 3. Process the HTTP/2 stream asynchronously
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

            // 4. Atomically commit the snapshot
            await transaction.CommitAsync(cancellationToken);
            _logger.LogInformation("Snapshot Ingestion successful! Committed {Count} rows cleanly.", processedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Pipeline failed. Rolling back snapshot transaction states.");
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }
}