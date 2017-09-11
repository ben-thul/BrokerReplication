USE [$(DBName)];
GO

CREATE TRIGGER [Sales].[enqueueInvoiceMessage]
    on [Sales].[Invoices]
    AFTER INSERT, UPDATE, DELETE
AS
BEGIN

    DECLARE @operation    CHAR(1) ,
            @ContractName sysname ,
            @MessageType  sysname ,
            @version      TINYINT = 1 , -- version the schema we're sending
            @message      XML;

    IF EXISTS (SELECT 1 FROM [Deleted])
    BEGIN
        IF EXISTS (SELECT 1 FROM [Inserted]) --is an update
            SELECT @operation = 'U',
                @ContractName = 'UpdateContract',
                @MessageType = 'InvoiceUpdate';
        ELSE -- is a delete
            SELECT @operation = 'D',
                @ContractName = 'DeleteContract',
                @MessageType = 'InvoiceDelete';
    END
    ELSE -- is an insert
        SELECT @operation = 'I',
            @ContractName = 'InsertContract',
            @MessageType = 'InvoiceInsert';

    IF (@operation IN ('I', 'U')) --is an insert or an update
    BEGIN
        SET @message = (
            SELECT [@v] = @version, 
                [@__mask__] = COLUMNS_UPDATED(),
                [@__operation__] = @operation,
            (
                SELECT 
                    [@InvoiceID] = [i].[InvoiceID] , -- PK; always send
                    [@CustomerID] = CASE WHEN UPDATE([CustomerID]) THEN [i].[CustomerID] ELSE NULL END,
                    [@BillToCustomerID] = CASE WHEN UPDATE([BillToCustomerID]) THEN [i].[BillToCustomerID] ELSE NULL END,
                    [@OrderID] = CASE WHEN UPDATE([OrderID]) THEN [i].[OrderID] ELSE NULL END,
                    [@DeliveryMethodID] = CASE WHEN UPDATE([DeliveryMethodID]) THEN [i].[DeliveryMethodID] ELSE NULL END,
                    [@ContactPersonID] = CASE WHEN UPDATE([ContactPersonID]) THEN [i].[ContactPersonID] ELSE NULL END,
                    [@AccountsPersonID] = CASE WHEN UPDATE([AccountsPersonID]) THEN [i].[AccountsPersonID] ELSE NULL END,
                    [@SalespersonPersonID] = CASE WHEN UPDATE([SalespersonPersonID]) THEN [i].[SalespersonPersonID] ELSE NULL END,
                    [@PackedByPersonID] = CASE WHEN UPDATE([PackedByPersonID]) THEN [i].[PackedByPersonID] ELSE NULL END,
                    [@InvoiceDate] = CASE WHEN UPDATE([InvoiceDate]) THEN [i].[InvoiceDate] ELSE NULL END,
                    [@CustomerPurchaseOrderNumber] = CASE WHEN UPDATE([CustomerPurchaseOrderNumber]) THEN CAST([i].[CustomerPurchaseOrderNumber] as VARBINARY(40)) ELSE NULL END,
                    [@IsCreditNote] = CASE WHEN UPDATE([IsCreditNote]) THEN [i].[IsCreditNote] ELSE NULL END,
                    [@CreditNoteReason] = CASE WHEN UPDATE([CreditNoteReason]) THEN CAST([i].[CreditNoteReason] as VARBINARY(MAX)) ELSE NULL END,
                    [@Comments] = CASE WHEN UPDATE([Comments]) THEN CAST([i].[Comments] as VARBINARY(MAX)) ELSE NULL END,
                    [@DeliveryInstructions] = CASE WHEN UPDATE([DeliveryInstructions]) THEN CAST([i].[DeliveryInstructions] as VARBINARY(MAX)) ELSE NULL END,
                    [@InternalComments] = CASE WHEN UPDATE([InternalComments]) THEN CAST([i].[InternalComments] as VARBINARY(MAX)) ELSE NULL END,
                    [@TotalDryItems] = CASE WHEN UPDATE([TotalDryItems]) THEN [i].[TotalDryItems] ELSE NULL END,
                    [@TotalChillerItems] = CASE WHEN UPDATE([TotalChillerItems]) THEN [i].[TotalChillerItems] ELSE NULL END,
                    [@DeliveryRun] = CASE WHEN UPDATE([DeliveryRun]) THEN CAST([i].[DeliveryRun] as VARBINARY(10)) ELSE NULL END,
                    [@RunPosition] = CASE WHEN UPDATE([RunPosition]) THEN CAST([i].[RunPosition] as VARBINARY(10)) ELSE NULL END,
                    [@ReturnedDeliveryData] = CASE WHEN UPDATE([ReturnedDeliveryData]) THEN CAST([i].[ReturnedDeliveryData] as VARBINARY(MAX)) ELSE NULL END,

                    -- The following two are computed columns; 
                    -- they will be sent implicitly with the data upon which they depend

                    -- [@ConfirmedDeliveryTime] = CASE WHEN UPDATE([ConfirmedDeliveryTime]) THEN [i].[ConfirmedDeliveryTime] ELSE NULL END,
                    -- [@ConfirmedReceivedBy] = CASE WHEN UPDATE([ConfirmedReceivedBy]) THEN CAST([i].[ConfirmedReceivedBy] as VARBINARY(8000)) ELSE NULL END,
                    [@LastEditedBy] = CASE WHEN UPDATE([LastEditedBy]) THEN [i].[LastEditedBy] ELSE NULL END,
                    [@LastEditedWhen] = CASE WHEN UPDATE([LastEditedWhen]) THEN [i].[LastEditedWhen] ELSE NULL END
                FROM [Inserted] AS [i]
                FOR XML PATH('Invoice'), TYPE, BINARY BASE64
            )
            FOR XML PATH('Invoices'), TYPE, BINARY BASE64
        );
    END
    ELSE
    BEGIN
        SET @message = (
            SELECT [@v] = @version,
                [@__operation__] = @operation,
                (
                    SELECT
                        [@InvoiceID] = [InvoiceID] -- PK; always send
                    FROM [Deleted]
                    FOR XML PATH('Invoice'), TYPE, BINARY BASE64
                ) 
            FOR XML PATH('Invoices'), TYPE, BINARY BASE64
        )
    END
    
    IF (@operation IN ('I', 'U', 'D'))
        EXEC [repl].[SendMessage]
            @ContractName = @ContractName,
            @MessageType = @MessageType,
            @message = @message;

END
GO

CREATE TRIGGER [Sales].[enqueueOrderMessage]
    on [Sales].[Orders]
    AFTER INSERT, UPDATE, DELETE
AS
BEGIN

    DECLARE @operation    CHAR(1) ,
            @ContractName sysname ,
            @MessageType  sysname ,
            @version      TINYINT = 1 , -- version the schema we're sending
            @message      XML;

    IF EXISTS (SELECT 1 FROM [Deleted])
    BEGIN
        IF EXISTS (SELECT 1 FROM [Inserted]) --is an update
            SELECT @operation = 'U',
                @ContractName = 'UpdateContract',
                @MessageType = 'OrderUpdate';
        ELSE -- is a delete
            SELECT @operation = 'D',
                @ContractName = 'DeleteContract',
                @MessageType = 'OrderDelete';
    END
    ELSE -- is an insert
        SELECT @operation = 'I',
            @ContractName = 'InsertContract',
            @MessageType = 'OrderInsert';

    IF (@operation IN ('I', 'U')) --is an insert or an update
    BEGIN
        SET @message = (
            SELECT [@v] = @version, 
                [@__mask__] = COLUMNS_UPDATED(),
                [@__operation__] = @operation,
            (
                SELECT 
                    [@OrderID] = [i].[OrderID] , -- PK; always send
                    [@CustomerID] = CASE WHEN UPDATE([CustomerID]) THEN [i].[CustomerID] ELSE NULL END,
                    [@SalespersonPersonID] = CASE WHEN UPDATE([SalespersonPersonID]) THEN [i].[SalespersonPersonID] ELSE NULL END,
                    [@PickedByPersonID] = CASE WHEN UPDATE([PickedByPersonID]) THEN [i].[PickedByPersonID] ELSE NULL END,
                    [@ContactPersonID] = CASE WHEN UPDATE([ContactPersonID]) THEN [i].[ContactPersonID] ELSE NULL END,
                    [@BackorderOrderID] = CASE WHEN UPDATE([BackorderOrderID]) THEN [i].[BackorderOrderID] ELSE NULL END,
                    [@OrderDate] = CASE WHEN UPDATE([OrderDate]) THEN [i].[OrderDate] ELSE NULL END,
                    [@ExpectedDeliveryDate] = CASE WHEN UPDATE([ExpectedDeliveryDate]) THEN [i].[ExpectedDeliveryDate] ELSE NULL END,
                    [@CustomerPurchaseOrderNumber] = CASE WHEN UPDATE([CustomerPurchaseOrderNumber]) THEN CAST([i].[CustomerPurchaseOrderNumber] as VARBINARY(40)) ELSE NULL END,
                    [@IsUndersupplyBackordered] = CASE WHEN UPDATE([IsUndersupplyBackordered]) THEN [i].[IsUndersupplyBackordered] ELSE NULL END,
                    [@Comments] = CASE WHEN UPDATE([Comments]) THEN CAST([i].[Comments] as VARBINARY(MAX)) ELSE NULL END,
                    [@DeliveryInstructions] = CASE WHEN UPDATE([DeliveryInstructions]) THEN CAST([i].[DeliveryInstructions] as VARBINARY(MAX)) ELSE NULL END,
                    [@InternalComments] = CASE WHEN UPDATE([InternalComments]) THEN CAST([i].[InternalComments] as VARBINARY(MAX)) ELSE NULL END,
                    [@PickingCompletedWhen] = CASE WHEN UPDATE([PickingCompletedWhen]) THEN [i].[PickingCompletedWhen] ELSE NULL END,
                    [@LastEditedBy] = CASE WHEN UPDATE([LastEditedBy]) THEN [i].[LastEditedBy] ELSE NULL END,
                    [@LastEditedWhen] = CASE WHEN UPDATE([LastEditedWhen]) THEN [i].[LastEditedWhen] ELSE NULL END
                FROM [Inserted] AS [i]
                FOR XML PATH('Order'), TYPE, BINARY BASE64
            )
            FOR XML PATH('Orders'), TYPE, BINARY BASE64
        );
    END
    ELSE
    BEGIN
        SET @message = (
            SELECT [@v] = @version,
                [@__operation__] = @operation,
                (
                    SELECT
                        [@OrderID] = [OrderID] -- PK; always send
                    FROM [Deleted]
                    FOR XML PATH('Order'), TYPE, BINARY BASE64
                ) 
            FOR XML PATH('Orders'), TYPE, BINARY BASE64
        )
    END
    
    IF (@operation IN ('I', 'U', 'D'))
        EXEC [repl].[SendMessage]
            @ContractName = @ContractName,
            @MessageType = @MessageType,
            @message = @message;

END
GO