DECLARE @InvoiceID INT = 2;

SELECT 'publisher', COUNT(*) AS [count]
FROM [WorldWideImporters].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', COUNT(*) AS [count]
FROM [WorldWideImportersSub1].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', COUNT(*) AS [count]
FROM [WorldWideImportersSub2].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID;

DELETE [il]
FROM [WorldWideImporters].[Sales].[InvoiceLines] AS [il]
WHERE [il].[InvoiceID] = @InvoiceID;

DELETE [ct]
FROM WorldWideImporters.Sales.CustomerTransactions AS [ct]
WHERE [ct].[InvoiceID] = @InvoiceID;

DELETE [sit]
FROM WorldWideImporters.Warehouse.StockItemTransactions AS [sit]
WHERE [sit].[InvoiceID] = @InvoiceID;

DELETE [i]
FROM [WorldWideImporters].[Sales].[Invoices] AS [i]
WHERE [i].[InvoiceID] = @InvoiceID;

WAITFOR DELAY '0:00:01';

SELECT 'publisher', COUNT(*) AS [count]
FROM [WorldWideImporters].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', COUNT(*) AS [count]
FROM [WorldWideImportersSub1].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', COUNT(*) AS [count]
FROM [WorldWideImportersSub2].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID;