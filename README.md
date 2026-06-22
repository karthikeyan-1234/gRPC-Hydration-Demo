# gRPC - Hydration - Demo

## Project Overview

This repository contains a high-performance data synchronization and hydration proof-of-concept (POC) built on **.NET 10**. The architecture demonstrates how to stream large volumes of relational transaction data across boundaries without network bottlenecks or table-locking on the consumer side.

## Core Architecture Components

* **`SharedLib`**: A central contract project compiling the custom Protocol Buffers (`proto3`) schemas into generated C# stubs for both client and server roles.
* **`Microservice A` (The Producer)**: Exposes a dedicated, cleartext HTTP/2 gRPC endpoint. It extracts 1,000 heavy transaction payloads from SQL Server using an un-tracked database cursor (`AsNoTracking`) and pipes them out via `IAsyncEnumerable`.
* **`Microservice B` (The Ingestion Engine)**: Consumes the gRPC network stream in real-time, chunks incoming payloads into memory batches of 500, and flushes them to the target database instance.

## Key Performance Patterns Monitored

* **Prior Knowledge Cleartext Channels**: Custom Kestrel pipeline configuration forcing a strict HTTP/2-only server topology to bypass ALPN negotiation bottlenecks over unencrypted development environments.
* **Non-Blocking Ingestion Loop**: Writes are enclosed inside an explicit **`SNAPSHOT` isolation transaction**. This provides all-or-nothing atomic data consistency while keeping the target tables completely readable by concurrent client applications during the hydration phase.
* **Fail-Fast Distributed Locking**: Implements database-level session application locks (`sp_getapplock`) to prevent concurrent race conditions across scaled-out instances without needing external infrastructure like Redis.

## Tech Stack

* **Runtime:** .NET 10
* **Protocols:** gRPC (HTTP/2) / Protocol Buffers (`proto3`)
* **Data Access:** Entity Framework Core 10 (SQL Server Provider)
* **Isolation Level:** Snapshot Isolation (Row Versioning via `tempdb`)

---

## Local Infrastructure Setup (Docker)

To run this simulation locally, you need two isolated SQL Server instances running on different host ports to prevent connection string collisions.

Execute the following commands in your terminal to spin up the required containers:

```bash
# Spin up SQL Server for Microservice A (Producer) on Port 1433
docker run -e "ACCEPT_EULA=Y" \
           -e "MSSQL_SA_PASSWORD=Hydration!Pass123" \
           -p 1433:1433 \
           --name mssql-service-a \
           -d mcr.microsoft.com/mssql/server:2022-latest

# Spin up SQL Server for Microservice B (Ingestion Engine) on Port 1434
docker run -e "ACCEPT_EULA=Y" \
           -e "MSSQL_SA_PASSWORD=Hydration!Pass123" \
           -p 1434:1433 \
           --name mssql-service-b \
           -d mcr.microsoft.com/mssql/server:2022-latest

```

---

## Database Initialization & Seeding Scripts

Connect to your local database instances using your preferred database tool (SSMS, Azure Data Studio, etc.) and execute the following scripts.

### 1. Setup Microservice A Database (Port 1433)

Connect to **`localhost,1433`** using the credentials `sa` / `Hydration!Pass123`. Run this script to generate the database, structure the source table, and generate exactly 1,000 transactional booking records for the pipeline to extract.

```sql
CREATE DATABASE MicroserviceADb;
GO

USE MicroserviceADb;
GO

CREATE TABLE Bookings (
    BookingId INT IDENTITY(1,1) PRIMARY KEY,
    MemberName VARCHAR(100) NOT NULL,
    ClassName VARCHAR(100) NOT NULL,
    ScheduleDate DATETIME NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    CreatedAt DATETIME DEFAULT GETDATE()
);
GO

-- Seed exactly 1,000 heavy transactional payload records
SET NOCOUNT ON;
DECLARE @Counter INT = 1;

WHILE @Counter <= 1000
BEGIN
    INSERT INTO Bookings (MemberName, ClassName, ScheduleDate, Price)
    VALUES (
        'Member_Name_' + CAST(@Counter AS VARCHAR(10)),
        CASE WHEN @Counter % 3 = 0 THEN 'HIIT Functional Circuit'
             WHEN @Counter % 3 = 1 THEN 'Strength & Conditioning'
             ELSE 'Cardio Blast Pro' END,
        DATEADD(DAY, (@Counter % 30), GETDATE()),
        45.00 + (@Counter % 5)
    );
    SET @Counter = @Counter + 1;
END;
GO

-- Verify rows have seeded correctly
SELECT COUNT(*) AS [Total Seeded Records] FROM Bookings;
GO

```

### 2. Setup Microservice B Database (Port 1434)

Connect to **`localhost,1434`** using the credentials `sa` / `Hydration!Pass123`. Run this script to generate the target ingestion database, turn on **Snapshot Isolation** capability, and configure the clean ingestion landing table.

```sql
CREATE DATABASE MicroserviceBDb;
GO

-- CRITICAL STEP: Turn on Row Versioning for Snapshot Isolation
ALTER DATABASE MicroserviceBDb SET ALLOW_SNAPSHOT_ISOLATION ON;
GO

USE MicroserviceBDb;
GO

CREATE TABLE SyncedBookings (
    BookingId INT PRIMARY KEY, -- Maintained from source service mapping
    MemberName VARCHAR(100) NOT NULL,
    ClassName VARCHAR(100) NOT NULL,
    ScheduleDate DATETIME NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    SyncedAt DATETIME DEFAULT GETDATE()
);
GO

```

---

## Multi-Instance Scale & Distributed Locking

When scaling this architecture to run across multiple instances (such as a scaled-out cloud profile or Kubernetes cluster), the application handles networking and database isolation dynamically.

### gRPC Multi-Instance Routing

* **Connection Pinning:** Because gRPC communicates via long-lived, multiplexed HTTP/2 streams, when an instance of Microservice B initializes a pipeline run, the Layer 7 load balancer maps that specific stream end-to-end to **one** instance of Microservice A.
* The remaining scaled-out worker instances sit completely idle, ensuring network stability over the lifetime of the ingestion run.

### The Database Concurrency Problem

Because the pipeline uses a `TRUNCATE TABLE` workflow followed by a massive transaction stream inside a `SNAPSHOT` boundary, concurrent manual triggers on multiple instances of Microservice B would create a structural race condition. Two instances attempting to truncate and update identical rows simultaneously will cause SQL Server to throw an **Update Conflict (Error 3960)**, crashing the sync.

### Zero-Infrastructure Solution: SQL Server Application Locks

To guarantee absolute singleton execution without introducing external infrastructure components (like Redis or Consul), Microservice B implements an **Exclusive Session-Level Application Lock** using SQL Server's internal `sp_getapplock` stored procedure.

* **Fail-Fast Enforcement:** Before opening the `SNAPSHOT` transaction, Microservice B requests a lock for a custom resource token (`BookingHydrationLock`) with a `@LockTimeout = 0`.
* **Behavior:** If Instance B-1 is already running the sync, and Instance B-2 receives a concurrent request, Instance B-2 immediately fails to acquire the token and returns an HTTP status code **`423 Locked`** back to the initiator without blocking threads or touching database tables.
* **Crash-Safe Release:** Because the lock is tied strictly to the underlying database connection session, if an active worker crashes mid-stream, SQL Server automatically drops the dead connection session and frees the lock resource instantly for subsequent retries.

---

## Azure App Service Architecture & Cloud Routing

When migrating this architecture from `localhost` to managed cloud infrastructure on Azure App Service, unencrypted HTTP/2 loopback channels are no longer viable. Traffic must pass through Azure's Front-End Proxy (Layer 7 Load Balancer), which enforces strict routing behaviors depending on the protocol.

### The Role of `HTTP20_ONLY_PORT`

Because cleartext HTTP/1.1 and HTTP/2 cannot automatically negotiate on a single unencrypted port without TLS (ALPN), Azure separates incoming public requests into two distinct internal streams.

When **gRPC Only** proxying is enabled, Azure passes public HTTPS requests on port 443 down to the application container via two completely separate unencrypted lanes:

* **Standard Web/REST Traffic:** Forwarded as HTTP/1.1 to the default container web port.
* **gRPC Streaming Traffic:** Forwarded as **Strict HTTP/2 Cleartext** to a dedicated port chosen dynamically by the host worker.

Azure automatically injects the chosen port number into the container environment via a variable named `HTTP20_ONLY_PORT`. Microservice A's `Program.cs` reads this variable dynamically to bind Kestrel directly to Azure's HTTP/2 ingestion channel.

---

## Azure Hosting Instructions

Follow these step-by-step configurations to deploy both microservices successfully without code changes.

### 1. Microservice A Configuration (The Server / Producer)

Microservice A must be configured to receive and decode the unencrypted HTTP/2 proxy traffic.

* **Operating System Requirement:** Deploy to a **Linux App Service Plan** (required for optimal native gRPC routing stability).
* **Protocol Version Setup:** 1. In the Azure Portal, navigate to your App Service $\rightarrow$ **Configuration** $\rightarrow$ **General Settings**.
2. Set **HTTP Version** to **2.0**.
3. Set **HTTP 2.0 Proxy** to **gRPC Only**.
* **Environment Variables:** The proxy routing automatically exposes `HTTP20_ONLY_PORT` to the underlying Linux container. The application code dynamically evaluates this variable at startup to switch from local port `8585` over to Azure's assigned internal routing port.

### 2. Microservice B Configuration (The Client / Ingestion Engine)

Microservice B acts strictly as an outbound client for gRPC. It does not require any specialized proxy or port mapping, but it must be instructed to point to the secure production endpoint instead of `localhost`.

* **App Settings / Environment Variables:**
Navigate to Microservice B's **Environment Variables** (or Configuration) panel in the Azure Portal and add the following override:
* **Name:** `GrpcServices__ServiceAUrl` *(Note the double underscore used for configuration key nesting)*
* **Value:** `https://your-microservice-a-name.azurewebsites.net`



### 3. Execution Verification

Once deployed, public REST triggers hitting Microservice B will dynamically resolve the environment configuration:

1. Microservice B evaluates the `https://` prefix, disabling the local unencrypted HTTP/2 socket switches.
2. The gRPC call traverses the public Azure Front-End proxy over secure HTTPS.
3. The proxy strips the TLS wrapper and funnels the payload into Microservice A's active `HTTP20_ONLY_PORT` container socket, running the snapshot synchronization workflow seamlessly in the cloud.
