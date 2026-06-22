using Grpc.Core;
using Hydration.Grpc; // <-- CRITICAL: Change this to match your proto namespaceusing Grpc.Core;
using MicroService_A.Data;
using Microsoft.EntityFrameworkCore;
using System.Net.NetworkInformation;

namespace MicroserviceA.Services;

public class DataHydrationService : DataHydration.DataHydrationBase
{
    private readonly ServiceADbContext _dbContext;
    private readonly ILogger<DataHydrationService> _logger;

    public DataHydrationService(ServiceADbContext dbContext, ILogger<DataHydrationService> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    public override async Task StreamData(
        HydrationRequest request,
        IServerStreamWriter<HydrationPayload> responseStream,
        ServerCallContext context)
    {
        _logger.LogInformation("gRPC Data Ingestion Stream requested. Batch profiling initiated.");

        try
        {
            // Fetch records asynchronously with zero tracking overhead
            var bookingStream = _dbContext.ClassBookings
                .AsNoTracking()
                .Select(booking => new HydrationPayload
                {
                    Id = booking.Id.ToString(),
                    PayloadData = $"ClassId: {booking.ClassId} | Status: {booking.Status} | Metadata: {booking.AdditionalMetadata}"
                })
                .AsAsyncEnumerable();

            int recordCount = 0;

            // Stream records chunk-by-chunk down the open TCP pipe
            await foreach (var record in bookingStream.WithCancellation(context.CancellationToken))
            {
                await responseStream.WriteAsync(record);
                recordCount++;
            }

            _logger.LogInformation("Successfully streamed {Count} transaction payloads to client.", recordCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("The data stream operation was cancelled by the client consumer.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "A fatal exception occurred during execution of the server data stream.");
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }
}