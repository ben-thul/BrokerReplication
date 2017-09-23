USE [WorldWideImporters]
GO

DECLARE @InvoiceID INT = 1;

UPDATE [Sales].[Invoices]
SET [InvoiceDate] = DATEADD(DAY, 1, [InvoiceDate])
WHERE [InvoiceID] = @InvoiceID;

WAITFOR DELAY '0:00:01';

SELECT 'publisher', [InvoiceDate]
FROM [WorldWideImporters].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber1', [InvoiceDate]
FROM [WorldWideImportersSub1].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID

UNION ALL

SELECT 'subscriber2', [InvoiceDate]
FROM [WorldWideImportersSub2].[Sales].[Invoices]
WHERE [InvoiceID] = @InvoiceID;