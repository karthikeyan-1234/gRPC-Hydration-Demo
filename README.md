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

## Tech Stack

* **Runtime:** .NET 10
* **Protocols:** gRPC (HTTP/2) / Protocol Buffers (`proto3`)
* **Data Access:** Entity Framework Core 10 (SQL Server Provider)
* **Isolation Level:** Snapshot Isolation (Row Versioning via `tempdb`)

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
