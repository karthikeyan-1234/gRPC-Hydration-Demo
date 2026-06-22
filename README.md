# gRPC - Hydration - Demo

## Project Overview
This repository contains a high-performance data synchronization and hydration proof-of-concept (POC) built on .NET 10. The architecture demonstrates how to stream large volumes of relational transaction data across boundaries without network bottlenecks or table-locking on the consumer side.

## Core Architecture Components
SharedLib: A central contract project compiling the custom Protocol Buffers (proto3) schemas into generated C# stubs for both client and server roles.

Microservice A (The Producer): Exposes a dedicated, cleartext HTTP/2 gRPC endpoint. It extracts 1,000 heavy transaction payloads from SQL Server using an un-tracked database cursor (AsNoTracking) and pipes them out via IAsyncEnumerable.

Microservice B (The Ingestion Engine): Consumes the gRPC network stream in real-time, chunks incoming payloads into memory batches of 500, and flushes them to the target database instance.

## Key Performance Patterns Monitored
Prior Knowledge Cleartext Channels: Custom Kestrel pipeline configuration forcing a strict HTTP/2-only server topology to bypass ALPN negotiation bottlenecks over unencrypted development environments.

Non-Blocking Ingestion Loop: Writes are enclosed inside an explicit SNAPSHOT isolation transaction. This provides all-or-nothing atomic data consistency while keeping the target tables completely readable by concurrent client applications during the hydration phase.

## Tech Stack
Runtime: .NET 10

Protocols: gRPC (HTTP/2) / Protocol Buffers (proto3)

Data Access: Entity Framework Core 10 (SQL Server Provider)

Isolation Level: Snapshot Isolation (Row Versioning via tempdb)
