using MicroService_A.Data;
using MicroserviceA.Services; // <-- Ensures DataHydrationService is visible
using Microsoft.EntityFrameworkCore;

using Microsoft.AspNetCore.Server.Kestrel.Core; // <-- CRITICAL: Add this using directive

var builder = WebApplication.CreateBuilder(args);

// Force Kestrel to lock port 5001 down to HTTP/2 ONLY
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(5001, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

// 1. Add EF Core with the connection string pointing to your local Docker container (Port 1433)
var connectionString = "Server=localhost,1433;Database=MicroserviceADb;User Id=sa;Password=Hydration!Pass123;TrustServerCertificate=True;";
builder.Services.AddDbContext<ServiceADbContext>(options =>
    options.UseSqlServer(connectionString));

// 2. Enable the gRPC framework components natively
builder.Services.AddGrpc();

var app = builder.Build();

// 3. Map your gRPC service implementation (we will create this next)
app.MapGrpcService<DataHydrationService>();

app.MapGet("/", () => "Microservice A gRPC Server is running active HTTP/2 streaming pipelines.");

app.Run();