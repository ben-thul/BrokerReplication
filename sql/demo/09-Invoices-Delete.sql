DECLARE @InvoiceID INT = 
    NEXT VALUE FOR [WorldWideImporters].[Sequences].[InvoiceID];

INSERT INTO [WorldWideImporters].[Sales].[Invoices]
(
    [InvoiceID] ,
    [CustomerID] ,
    [BillToCustomerID] ,
    [OrderID] ,
    [DeliveryMethodID] ,
    [ContactPersonID] ,
    [AccountsPersonID] ,
    [SalespersonPersonID] ,
    [PackedByPersonID] ,
    [InvoiceDate] ,
    [CustomerPurchaseOrderNumber] ,
    [IsCreditNote] ,
    [CreditNoteReason] ,
    [Comments] ,
    [DeliveryInstructions] ,
    [InternalComments] ,
    [TotalDryItems] ,
    [TotalChillerItems] ,
    [DeliveryRun] ,
    [RunPosition] ,
    [ReturnedDeliveryData] ,
    [LastEditedBy] ,
    [LastEditedWhen]
)
SELECT
    @InvoiceID ,           -- InvoiceID - int
    [CustomerID] ,
    [BillToCustomerID] ,
    [OrderID] ,
    [DeliveryMethodID] ,
    [ContactPersonID] ,
    [AccountsPersonID] ,
    [SalespersonPersonID] ,
    [PackedByPersonID] ,
    [InvoiceDate] ,
    [CustomerPurchaseOrderNumber] ,
    [IsCreditNote] ,
    [CreditNoteReason] ,
    [Comments] ,
    [DeliveryInstructions] ,
    [InternalComments] ,
    [TotalDryItems] ,
    [TotalChillerItems] ,
    [DeliveryRun] ,
    [RunPosition] ,
    [ReturnedDeliveryData] ,
    [LastEditedBy] ,
    [LastEditedWhen]
FROM [WorldWideImporters].[Sales].[Invoices] AS [i]
WHERE [i].[InvoiceID] = 1;

WAITFOR DELAY '0:00:01';

SELECT 'publisher', [i].[InvoiceID], CHECKSUM(*)
FROM [WorldWideImporters].[Sales].[Invoices] AS [i]
WHERE [i].[InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', [i].[InvoiceID], CHECKSUM(*)
FROM [WorldWideImportersSub1].[Sales].[Invoices] AS [i]
WHERE [i].[InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', [i].[InvoiceID], CHECKSUM(*)
FROM [WorldWideImportersSub2].[Sales].[Invoices] AS [i]
WHERE [i].[InvoiceID] = @InvoiceID;