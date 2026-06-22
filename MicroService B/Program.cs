using Hydration.Grpc;
using MicroService_B.Data;
using MicroserviceB.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Register DB Context pointing to the target SQL Instance (Port 1434)
var connectionString = "Server=localhost,1434;Database=MicroserviceBDb;User Id=sa;Password=Hydration!Pass123;TrustServerCertificate=True;";
builder.Services.AddDbContext<ServiceBDbContext>(options =>
    options.UseSqlServer(connectionString));

// Register the gRPC Client linked directly to Microservice A's endpoint
builder.Services.AddGrpcClient<DataHydration.DataHydrationClient>(o =>
{
    o.Address = new Uri("http://localhost:5001");
});
// Allow HttpClient to send unencrypted HTTP/2 payloads without TLS verification
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);


// Register our processing pipeline engine
builder.Services.AddScoped<DataSyncProcessor>();

var app = builder.Build();

// Expose a quick trigger endpoint to execute our POC sync pipeline manually
app.MapPost("/api/sync-trigger", async (DataSyncProcessor processor, CancellationToken ct) =>
{
    try
    {
        await processor.RunHydrationPipelineAsync(ct);
        return Results.Ok(new { Status = "Success", Message = "Data stream synchronized under Snapshot Transaction." });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Sync Pipeline Interrupted: {ex.Message}");
    }
});

app.Run();