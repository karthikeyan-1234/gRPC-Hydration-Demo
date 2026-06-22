using Hydration.Grpc;
using MicroService_B.Data;
using MicroserviceB.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// 1. Extract endpoint from appsettings JSON (falls back to port 8585 if missing)
var serviceAUrl = builder.Configuration["GrpcServices:ServiceAUrl"] ?? "http://localhost:8585";

// 2. Only enable unencrypted cleartext switches if testing over unencrypted HTTP channels locally
if (serviceAUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
{
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
}

var connectionString = "Server=localhost,1434;Database=MicroserviceBDb;User Id=sa;Password=Hydration!Pass123;TrustServerCertificate=True;";
builder.Services.AddDbContext<ServiceBDbContext>(options =>
    options.UseSqlServer(connectionString));

// 3. Register gRPC client pointing to the dynamic configuration-driven URL
builder.Services.AddGrpcClient<DataHydration.DataHydrationClient>(o =>
{
    o.Address = new Uri(serviceAUrl);
});

builder.Services.AddScoped<DataSyncProcessor>();

var app = builder.Build();
app.MapPost("/api/sync-trigger", async (DataSyncProcessor processor, CancellationToken ct) =>
{
    try
    {
        await processor.RunHydrationPipelineAsync(ct);
        return Results.Ok(new { Status = "Success", Message = "Data synchronized." });
    }
    catch (Exception ex)
    {
        return Results.Problem($"Sync Pipeline Interrupted: {ex.Message}");
    }
});

app.Run();