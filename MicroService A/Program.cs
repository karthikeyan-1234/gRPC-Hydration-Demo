using MicroService_A.Data;
using MicroserviceA.Services;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Dynamic Cloud-Aware Port Binding
builder.WebHost.ConfigureKestrel(options =>
{
    var azureHttp2Port = Environment.GetEnvironmentVariable("HTTP20_ONLY_PORT");

    if (!string.IsNullOrEmpty(azureHttp2Port) && int.TryParse(azureHttp2Port, out var port))
    {
        // Executes locally during simulation OR natively inside Azure App Service Linux container
        options.ListenAnyIP(port, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http2;
        });
    }
    else
    {
        // Legacy local fallback if no environment variable is present
        options.ListenLocalhost(5001, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http2;
        });
    }
});

var connectionString = "Server=localhost,1433;Database=MicroserviceADb;User Id=sa;Password=Hydration!Pass123;TrustServerCertificate=True;";
builder.Services.AddDbContext<ServiceADbContext>(options =>
    options.UseSqlServer(connectionString));

builder.Services.AddGrpc();

var app = builder.Build();
app.MapGrpcService<DataHydrationService>();
app.MapGet("/", () => "Microservice A gRPC Server Active.");
app.Run();