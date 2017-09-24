USE [$(DBName)];
GO

ALTER DATABASE [$(DBName)] SET NEW_BROKER WITH ROLLBACK IMMEDIATE;
GO

CREATE SCHEMA [repl] AUTHORIZATION [ReplUser];
GO

CREATE MESSAGE TYPE [InvoiceInsert] VALIDATION = NONE;
CREATE MESSAGE TYPE [OrderInsert] VALIDATION = NONE;

CREATE CONTRACT [InsertContract] (
    [InvoiceInsert] SENT BY INITIATOR,
    [OrderInsert] SENT BY INITIATOR
);

CREATE MESSAGE TYPE [InvoiceUpdate] VALIDATION = NONE;
CREATE MESSAGE TYPE [OrderUpdate] VALIDATION = NONE;

CREATE CONTRACT [UpdateContract] (
    [InvoiceUpdate] SENT BY INITIATOR,
    [OrderUpdate] SENT BY INITIATOR
);

CREATE MESSAGE TYPE [InvoiceDelete] VALIDATION = NONE;
CREATE MESSAGE TYPE [OrderDelete] VALIDATION = NONE;

CREATE CONTRACT [DeleteContract] (
    [InvoiceDelete] SENT BY INITIATOR,
    [OrderDelete] SENT BY INITIATOR
);

CREATE MESSAGE TYPE [SchemaSync] VALIDATION = NONE;
CREATE CONTRACT [SchemaSync] ([SchemaSync] SENT BY INITIATOR);

CREATE QUEUE [repl].[ReplicationQueue];
CREATE SERVICE [ReplicationService]
    AUTHORIZATION [ReplUser]
    ON QUEUE [repl].[ReplicationQueue] (
        [InsertContract],
        [UpdateContract],
        [DeleteContract],
        [SchemaSync]
    );

DROP ROUTE [AutoCreatedLocal];

CREATE REMOTE SERVICE BINDING [ReplicationServiceBinding] 
    TO SERVICE 'ReplicationService'
    WITH USER = [ReplUser];