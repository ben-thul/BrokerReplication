USE [$(DBName)];
GO

CREATE TABLE [repl].[DeadLetters] (
    [ID] INT IDENTITY NOT NULL,
        CONSTRAINT [PK_DeadLetters] PRIMARY KEY CLUSTERED (ID),
    [Payload] VARBINARY(MAX), 
    [MessageType] sysname,
    [TS] DATETIME2(3) NOT NULL CONSTRAINT [DF_DeadLetters__TS] DEFAULT SYSUTCDATETIME()
)
GO

CREATE TABLE dbo.Numbers (n INT NOT NULL);
CREATE UNIQUE CLUSTERED INDEX [CIX_Numbers] ON dbo.Numbers (n);
SET NOCOUNT ON;

DECLARE @largest INT = 10000;
WHILE(1=1)
BEGIN
    WITH
      Pass0 as (select 1 as C union all select 1), --2 rows
      Pass1 as (select 1 as C from Pass0 as A, Pass0 as B),--4 rows
      Pass2 as (select 1 as C from Pass1 as A, Pass1 as B),--16 rows
      Pass3 as (select 1 as C from Pass2 as A, Pass2 as B),--256 rows
      Pass4 as (select 1 as C from Pass3 as A, Pass3 as B),--65536 rows
      Tally as (select row_number() over(order by C) as Number from Pass4)
    INSERT  INTO dbo.Numbers
            ( n )
    SELECT  TOP (10000) [t].[Number]
    FROM    Tally AS [t]
    LEFT JOIN dbo.Numbers AS [n]
            ON [t].[Number] = [n].[n]
    WHERE   Number <= @largest
            AND [n].[n] IS NULL;

    IF (@@rowCount < 10000)
        BREAK;
END
GO

CREATE TABLE [repl].[SubscribedColumns] (
    PublisherID UNIQUEIDENTIFIER NOT NULL,
    ObjectName sysname NOT NULL,
    ColumnName sysname NOT NULL,
    ColumnID INT NOT NULL,
    CONSTRAINT [PK_SubscribedColumns] PRIMARY KEY CLUSTERED ([PublisherID], [ObjectName], [ColumnName])
)

GO

CREATE FUNCTION [repl].[columnNamesFromUpdateMask](
    @PublisherID UNIQUEIDENTIFIER,
    @tablename sysname, 
    @updateMask varbinary(16)
)
RETURNS TABLE AS
RETURN
--adapted from http://sqlblogcasts.com/blogs/piotr_rodak/archive/2010/04/28/columns-updated.aspx

	with column_updated_bytesCTE
		as
		(
            --divide bitmask into bytes.
		    select n.n as ByteNumber, 
			    convert(
				    binary(1),
				    substring(@updateMask, n.n, 1)
			    ) as [ByteValue]
		    from dbo.Numbers AS n
		    where n.n <= datalength(@updateMask)
		),
		columnsCTE as
		(
            -- return columns belonging to table @tablename, 
            -- calculate appropriate bit masks
			select [sc].[ColumnID], 
				[sc].[ColumnName], 
				ByteNumber, 
				ByteValue,
				power(2, ((([sc].[ColumnID] - 1 ) % 8) + 1) - 1) as BitMask
			from [repl].[SubscribedColumns] as [sc]
			join column_updated_bytesCTE as b
				on (([sc].[ColumnID] - 1) / 8) + 1 = b.ByteNumber
			WHERE [sc].[PublisherID] = @PublisherID
                AND [sc].[ObjectName] = @tablename
		) 
		select [ColumnID], [ColumnName]		
		from columnsCTE
		where ByteValue & BitMask > 0
GO

CREATE PROCEDURE [repl].[ProcessInvoiceMessage] (
    @message XML
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @v TINYINT = @message.value('(/Invoices/@v)[1]', 'tinyint'),
        @update_mask varbinary(16) = @message.value('(/Invoices/@__mask__)[1]', 'varbinary(16)'),
        @operation CHAR(1) = @message.value('(/Invoices/@__operation__)[1]', 'char(1)'),
        @PublisherID UNIQUEIDENTIFIER = @message.value('(/Invoices/@PublisherID)[1]', 'UNIQUEIDENTIFIER');

    CREATE TABLE [#Invoices](
	    [InvoiceID] [int] NOT NULL,
	    [CustomerID] [int] NULL,
	    [BillToCustomerID] [int] NULL,
	    [OrderID] [int] NULL,
	    [DeliveryMethodID] [int] NULL,
	    [ContactPersonID] [int] NULL,
	    [AccountsPersonID] [int] NULL,
	    [SalespersonPersonID] [int] NULL,
	    [PackedByPersonID] [int] NULL,
	    [InvoiceDate] [date] NULL,
	    [CustomerPurchaseOrderNumber] [nvarchar](20) NULL,
	    [IsCreditNote] [bit] NULL,
	    [CreditNoteReason] [nvarchar](max) NULL,
	    [Comments] [nvarchar](max) NULL,
	    [DeliveryInstructions] [nvarchar](max) NULL,
	    [InternalComments] [nvarchar](max) NULL,
	    [TotalDryItems] [int] NULL,
	    [TotalChillerItems] [int] NULL,
	    [DeliveryRun] [nvarchar](5) NULL,
	    [RunPosition] [nvarchar](5) NULL,
	    [ReturnedDeliveryData] [nvarchar](max) NULL,
	    [LastEditedBy] [int] NULL,
	    [LastEditedWhen] [datetime2](7) NULL
    );

    IF (@v = 1)
    BEGIN
        WITH cte AS (
            SELECT n.value('(./@InvoiceID)[1]', 'int') as [InvoiceID],
                n.value('(./@CustomerID)[1]', 'int') as [CustomerID],
                n.value('(./@BillToCustomerID)[1]', 'int') as [BillToCustomerID],
                n.value('(./@OrderID)[1]', 'int') as [OrderID],
                n.value('(./@DeliveryMethodID)[1]', 'int') as [DeliveryMethodID],
                n.value('(./@ContactPersonID)[1]', 'int') as [ContactPersonID],
                n.value('(./@AccountsPersonID)[1]', 'int') as [AccountsPersonID],
                n.value('(./@SalespersonPersonID)[1]', 'int') as [SalespersonPersonID],
                n.value('(./@PackedByPersonID)[1]', 'int') as [PackedByPersonID],
                n.value('(./@InvoiceDate)[1]', 'date') as [InvoiceDate],
                n.value('(./@CustomerPurchaseOrderNumber)[1]', 'VARBINARY(40)') as [CustomerPurchaseOrderNumber],
                n.value('(./@IsCreditNote)[1]', 'bit') as [IsCreditNote],
                n.value('(./@CreditNoteReason)[1]', 'VARBINARY(MAX)') as [CreditNoteReason],
                n.value('(./@Comments)[1]', 'VARBINARY(MAX)') as [Comments],
                n.value('(./@DeliveryInstructions)[1]', 'VARBINARY(MAX)') as [DeliveryInstructions],
                n.value('(./@InternalComments)[1]', 'VARBINARY(MAX)') as [InternalComments],
                n.value('(./@TotalDryItems)[1]', 'int') as [TotalDryItems],
                n.value('(./@TotalChillerItems)[1]', 'int') as [TotalChillerItems],
                n.value('(./@DeliveryRun)[1]', 'VARBINARY(10)') as [DeliveryRun],
                n.value('(./@RunPosition)[1]', 'VARBINARY(10)') as [RunPosition],
                n.value('(./@ReturnedDeliveryData)[1]', 'VARBINARY(MAX)') as [ReturnedDeliveryData],
                n.value('(./@LastEditedBy)[1]', 'int') as [LastEditedBy],
                n.value('(./@LastEditedWhen)[1]', 'datetime2') as [LastEditedWhen]
            FROM @message.nodes('/Invoices/Invoice') AS x(n)
        )
        INSERT INTO [#Invoices]
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
        SELECT [cte].[InvoiceID] ,
               [cte].[CustomerID] ,
               [cte].[BillToCustomerID] ,
               [cte].[OrderID] ,
               [cte].[DeliveryMethodID] ,
               [cte].[ContactPersonID] ,
               [cte].[AccountsPersonID] ,
               [cte].[SalespersonPersonID] ,
               [cte].[PackedByPersonID] ,
               [cte].[InvoiceDate] ,
               [cte].[CustomerPurchaseOrderNumber] ,
               [cte].[IsCreditNote] ,
               [cte].[CreditNoteReason] ,
               [cte].[Comments] ,
               [cte].[DeliveryInstructions] ,
               [cte].[InternalComments] ,
               [cte].[TotalDryItems] ,
               [cte].[TotalChillerItems] ,
               [cte].[DeliveryRun] ,
               [cte].[RunPosition] ,
               [cte].[ReturnedDeliveryData] ,
               [cte].[LastEditedBy] ,
               [cte].[LastEditedWhen]
        FROM [cte]
    END

    BEGIN TRY
        IF ( @operation = 'U')
        BEGIN
            WITH updated_columns AS (
                SELECT [InvoiceID] ,
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
	            from (
		            select [ColumnName]
		            from repl.columnNamesFromUpdateMask(@PublisherID, 'Sales.Invoices', @update_mask)
	            ) as p
	            pivot (
		            count([ColumnName])
		            for [ColumnName] in (
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
	            ) as pvt
            )
            UPDATE  [target]
            SET [CustomerID] = CASE WHEN [updated_columns].[CustomerID] = 1 THEN [source].[CustomerID] ELSE [target].[CustomerID] END ,
                [BillToCustomerID] = CASE WHEN [updated_columns].[BillToCustomerID] = 1 THEN [source].[BillToCustomerID] ELSE [target].[BillToCustomerID] END ,
                [OrderID] = CASE WHEN [updated_columns].[OrderID] = 1 THEN [source].[OrderID] ELSE [target].[OrderID] END ,
                [DeliveryMethodID] = CASE WHEN [updated_columns].[DeliveryMethodID] = 1 THEN [source].[DeliveryMethodID] ELSE [target].[DeliveryMethodID] END ,
                [ContactPersonID] = CASE WHEN [updated_columns].[ContactPersonID] = 1 THEN [source].[ContactPersonID] ELSE [target].[ContactPersonID] END ,
                [AccountsPersonID] = CASE WHEN [updated_columns].[AccountsPersonID] = 1 THEN [source].[AccountsPersonID] ELSE [target].[AccountsPersonID] END ,
                [SalespersonPersonID] = CASE WHEN [updated_columns].[SalespersonPersonID] = 1 THEN [source].[SalespersonPersonID] ELSE [target].[SalespersonPersonID] END ,
                [PackedByPersonID] = CASE WHEN [updated_columns].[PackedByPersonID] = 1 THEN [source].[PackedByPersonID] ELSE [target].[PackedByPersonID] END ,
                [InvoiceDate] = CASE WHEN [updated_columns].[InvoiceDate] = 1 THEN [source].[InvoiceDate] ELSE [target].[InvoiceDate] END ,
                [CustomerPurchaseOrderNumber] = CASE WHEN [updated_columns].[CustomerPurchaseOrderNumber] = 1 THEN [source].[CustomerPurchaseOrderNumber] ELSE [target].[CustomerPurchaseOrderNumber] END ,
                [IsCreditNote] = CASE WHEN [updated_columns].[IsCreditNote] = 1 THEN [source].[IsCreditNote] ELSE [target].[IsCreditNote] END ,
                [CreditNoteReason] = CASE WHEN [updated_columns].[CreditNoteReason] = 1 THEN [source].[CreditNoteReason] ELSE [target].[CreditNoteReason] END ,
                [Comments] = CASE WHEN [updated_columns].[Comments] = 1 THEN [source].[Comments] ELSE [target].[Comments] END ,
                [DeliveryInstructions] = CASE WHEN [updated_columns].[DeliveryInstructions] = 1 THEN [source].[DeliveryInstructions] ELSE [target].[DeliveryInstructions] END ,
                [InternalComments] = CASE WHEN [updated_columns].[InternalComments] = 1 THEN [source].[InternalComments] ELSE [target].[InternalComments] END ,
                [TotalDryItems] = CASE WHEN [updated_columns].[TotalDryItems] = 1 THEN [source].[TotalDryItems] ELSE [target].[TotalDryItems] END ,
                [TotalChillerItems] = CASE WHEN [updated_columns].[TotalChillerItems] = 1 THEN [source].[TotalChillerItems] ELSE [target].[TotalChillerItems] END ,
                [DeliveryRun] = CASE WHEN [updated_columns].[DeliveryRun] = 1 THEN [source].[DeliveryRun] ELSE [target].[DeliveryRun] END ,
                [RunPosition] = CASE WHEN [updated_columns].[RunPosition] = 1 THEN [source].[RunPosition] ELSE [target].[RunPosition] END ,
                [ReturnedDeliveryData] = CASE WHEN [updated_columns].[ReturnedDeliveryData] = 1 THEN [source].[ReturnedDeliveryData] ELSE [target].[ReturnedDeliveryData] END ,
                [LastEditedBy] = CASE WHEN [updated_columns].[LastEditedBy] = 1 THEN [source].[LastEditedBy] ELSE [target].[LastEditedBy] END ,
                [LastEditedWhen] = CASE WHEN [updated_columns].[LastEditedWhen] = 1 THEN [source].[LastEditedWhen] ELSE [target].[LastEditedWhen] END
            FROM    [#Invoices] AS [source]
            JOIN    [Sales].[Invoices] AS [target]
                    ON [target].[InvoiceID] = [source].[InvoiceID]
            CROSS JOIN [updated_columns];
        END
        ELSE IF (@operation = 'I')
        BEGIN
                INSERT INTO [Sales].[Invoices]
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
                SELECT [source].[InvoiceID] ,
                       [source].[CustomerID] ,
                       [source].[BillToCustomerID] ,
                       [source].[OrderID] ,
                       [source].[DeliveryMethodID] ,
                       [source].[ContactPersonID] ,
                       [source].[AccountsPersonID] ,
                       [source].[SalespersonPersonID] ,
                       [source].[PackedByPersonID] ,
                       [source].[InvoiceDate] ,
                       [source].[CustomerPurchaseOrderNumber] ,
                       [source].[IsCreditNote] ,
                       [source].[CreditNoteReason] ,
                       [source].[Comments] ,
                       [source].[DeliveryInstructions] ,
                       [source].[InternalComments] ,
                       [source].[TotalDryItems] ,
                       [source].[TotalChillerItems] ,
                       [source].[DeliveryRun] ,
                       [source].[RunPosition] ,
                       [source].[ReturnedDeliveryData] ,
                       [source].[LastEditedBy] ,
                       [source].[LastEditedWhen]
                FROM [#Invoices] AS [source]
                LEFT JOIN [Sales].[Invoices] AS [target]
                    ON [target].[InvoiceID] = [source].[InvoiceID]
                WHERE [target].[InvoiceID] IS NULL;
        END
        ELSE IF (@operation = 'D')
        BEGIN
            DELETE [source]
            FROM [Sales].[Invoices] AS [source]
            JOIN [#Invoices] AS [target]
                ON [source].[InvoiceID] = [target].[InvoiceID];
        END
    END TRY
    BEGIN CATCH
        RETURN -1;
    END CATCH

    RETURN 0;
END
GO

CREATE PROCEDURE [repl].[ProcessOrderMessage] (
    @message XML
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @v TINYINT = @message.value('(/Orders/@v)[1]', 'tinyint'),
        @update_mask varbinary(16) = @message.value('(/Orders/@__mask__)[1]', 'varbinary(16)'),
        @operation CHAR(1) = @message.value('(/Orders/@__operation__)[1]', 'char(1)'),
        @PublisherID UNIQUEIDENTIFIER = @message.value('(/Invoices/@PublisherID)[1]', 'UNIQUEIDENTIFIER');

    CREATE TABLE [#Orders](
	    [OrderID] [int] NOT NULL,
	    [CustomerID] [int] NULL,
	    [SalespersonPersonID] [int] NULL,
	    [PickedByPersonID] [int] NULL,
	    [ContactPersonID] [int] NULL,
	    [BackorderOrderID] [int] NULL,
	    [OrderDate] [date] NULL,
	    [ExpectedDeliveryDate] [date] NULL,
	    [CustomerPurchaseOrderNumber] [nvarchar](20) NULL,
	    [IsUndersupplyBackordered] [bit] NULL,
	    [Comments] [nvarchar](max) NULL,
	    [DeliveryInstructions] [nvarchar](max) NULL,
	    [InternalComments] [nvarchar](max) NULL,
	    [PickingCompletedWhen] [datetime2](7) NULL,
	    [LastEditedBy] [int] NULL,
	    [LastEditedWhen] [datetime2](7) NULL
    );

    IF (@v = 1)
    BEGIN
        WITH cte AS (
            SELECT n.value('(./@OrderID)[1]', 'int') as [OrderID],
                n.value('(./@CustomerID)[1]', 'int') as [CustomerID],
                n.value('(./@SalespersonPersonID)[1]', 'int') as [SalespersonPersonID],
                n.value('(./@PickedByPersonID)[1]', 'int') as [PickedByPersonID],
                n.value('(./@ContactPersonID)[1]', 'int') as [ContactPersonID],
                n.value('(./@BackorderOrderID)[1]', 'int') as [BackorderOrderID],
                n.value('(./@OrderDate)[1]', 'date') as [OrderDate],
                n.value('(./@ExpectedDeliveryDate)[1]', 'date') as [ExpectedDeliveryDate],
                n.value('(./@CustomerPurchaseOrderNumber)[1]', 'VARBINARY(40)') as [CustomerPurchaseOrderNumber],
                n.value('(./@IsUndersupplyBackordered)[1]', 'bit') as [IsUndersupplyBackordered],
                n.value('(./@Comments)[1]', 'VARBINARY(MAX)') as [Comments],
                n.value('(./@DeliveryInstructions)[1]', 'VARBINARY(MAX)') as [DeliveryInstructions],
                n.value('(./@InternalComments)[1]', 'VARBINARY(MAX)') as [InternalComments],
                n.value('(./@PickingCompletedWhen)[1]', 'datetime2') as [PickingCompletedWhen],
                n.value('(./@LastEditedBy)[1]', 'int') as [LastEditedBy],
                n.value('(./@LastEditedWhen)[1]', 'datetime2') as [LastEditedWhen]
            FROM @message.nodes('/Orders/Order') AS x(n)
        )
        INSERT INTO [#Orders]
        (
            [OrderID] ,
            [CustomerID] ,
            [SalespersonPersonID] ,
            [PickedByPersonID] ,
            [ContactPersonID] ,
            [BackorderOrderID] ,
            [OrderDate] ,
            [ExpectedDeliveryDate] ,
            [CustomerPurchaseOrderNumber] ,
            [IsUndersupplyBackordered] ,
            [Comments] ,
            [DeliveryInstructions] ,
            [InternalComments] ,
            [PickingCompletedWhen] ,
            [LastEditedBy] ,
            [LastEditedWhen]
        )
        SELECT [cte].[OrderID] ,
               [cte].[CustomerID] ,
               [cte].[SalespersonPersonID] ,
               [cte].[PickedByPersonID] ,
               [cte].[ContactPersonID] ,
               [cte].[BackorderOrderID] ,
               [cte].[OrderDate] ,
               [cte].[ExpectedDeliveryDate] ,
               [cte].[CustomerPurchaseOrderNumber] ,
               [cte].[IsUndersupplyBackordered] ,
               [cte].[Comments] ,
               [cte].[DeliveryInstructions] ,
               [cte].[InternalComments] ,
               [cte].[PickingCompletedWhen] ,
               [cte].[LastEditedBy] ,
               [cte].[LastEditedWhen]
        FROM [cte];
    END

    BEGIN TRY
        IF ( @operation = 'U')
        BEGIN
            WITH updated_columns AS (
                SELECT [OrderID] ,
                       [CustomerID] ,
                       [SalespersonPersonID] ,
                       [PickedByPersonID] ,
                       [ContactPersonID] ,
                       [BackorderOrderID] ,
                       [OrderDate] ,
                       [ExpectedDeliveryDate] ,
                       [CustomerPurchaseOrderNumber] ,
                       [IsUndersupplyBackordered] ,
                       [Comments] ,
                       [DeliveryInstructions] ,
                       [InternalComments] ,
                       [PickingCompletedWhen] ,
                       [LastEditedBy] ,
                       [LastEditedWhen]
	            from (
		            select [ColumnName]
		            from repl.columnNamesFromUpdateMask(@PublisherID, 'Sales.Orders', @update_mask)
	            ) as p
	            pivot (
		            count([ColumnName])
		            for [ColumnName] in (
                       [OrderID] ,
                       [CustomerID] ,
                       [SalespersonPersonID] ,
                       [PickedByPersonID] ,
                       [ContactPersonID] ,
                       [BackorderOrderID] ,
                       [OrderDate] ,
                       [ExpectedDeliveryDate] ,
                       [CustomerPurchaseOrderNumber] ,
                       [IsUndersupplyBackordered] ,
                       [Comments] ,
                       [DeliveryInstructions] ,
                       [InternalComments] ,
                       [PickingCompletedWhen] ,
                       [LastEditedBy] ,
                       [LastEditedWhen]
                    )
	            ) as pvt
            )
            UPDATE  [target]
            SET [CustomerID] = CASE WHEN [updated_columns].[CustomerID] = 1 THEN [source].[CustomerID] ELSE [target].[CustomerID] END ,
                [SalespersonPersonID] = CASE WHEN [updated_columns].[SalespersonPersonID] = 1 THEN [source].[SalespersonPersonID] ELSE [target].[SalespersonPersonID] END ,
                [PickedByPersonID] = CASE WHEN [updated_columns].[PickedByPersonID] = 1 THEN [source].[PickedByPersonID] ELSE [target].[PickedByPersonID] END ,
                [ContactPersonID] = CASE WHEN [updated_columns].[ContactPersonID] = 1 THEN [source].[ContactPersonID] ELSE [target].[ContactPersonID] END ,
                [BackorderOrderID] = CASE WHEN [updated_columns].[BackorderOrderID] = 1 THEN [source].[BackorderOrderID] ELSE [target].[BackorderOrderID] END ,
                [OrderDate] = CASE WHEN [updated_columns].[OrderDate] = 1 THEN [source].[OrderDate] ELSE [target].[OrderDate] END ,
                [ExpectedDeliveryDate] = CASE WHEN [updated_columns].[ExpectedDeliveryDate] = 1 THEN [source].[ExpectedDeliveryDate] ELSE [target].[ExpectedDeliveryDate] END ,
                [CustomerPurchaseOrderNumber] = CASE WHEN [updated_columns].[CustomerPurchaseOrderNumber] = 1 THEN [source].[CustomerPurchaseOrderNumber] ELSE [target].[CustomerPurchaseOrderNumber] END ,
                [IsUndersupplyBackordered] = CASE WHEN [updated_columns].[IsUndersupplyBackordered] = 1 THEN [source].[IsUndersupplyBackordered] ELSE [target].[IsUndersupplyBackordered] END ,
                [Comments] = CASE WHEN [updated_columns].[Comments] = 1 THEN [source].[Comments] ELSE [target].[Comments] END ,
                [DeliveryInstructions] = CASE WHEN [updated_columns].[DeliveryInstructions] = 1 THEN [source].[DeliveryInstructions] ELSE [target].[DeliveryInstructions] END ,
                [InternalComments] = CASE WHEN [updated_columns].[InternalComments] = 1 THEN [source].[InternalComments] ELSE [target].[InternalComments] END ,
                [PickingCompletedWhen] = CASE WHEN [updated_columns].[PickingCompletedWhen] = 1 THEN [source].[PickingCompletedWhen] ELSE [target].[PickingCompletedWhen] END ,
                [LastEditedBy] = CASE WHEN [updated_columns].[LastEditedBy] = 1 THEN [source].[LastEditedBy] ELSE [target].[LastEditedBy] END ,
                [LastEditedWhen] = CASE WHEN [updated_columns].[LastEditedWhen] = 1 THEN [source].[LastEditedWhen] ELSE [target].[LastEditedWhen] END 
            FROM    [#Orders] AS [source]
            JOIN    [Sales].[Orders] AS [target]
                    ON [target].[OrderID] = [source].[OrderID]
            CROSS JOIN [updated_columns];
        END
        ELSE IF (@operation = 'I')
        BEGIN
                INSERT INTO [Sales].[Orders]
                (
                    [OrderID] ,
                    [CustomerID] ,
                    [SalespersonPersonID] ,
                    [PickedByPersonID] ,
                    [ContactPersonID] ,
                    [BackorderOrderID] ,
                    [OrderDate] ,
                    [ExpectedDeliveryDate] ,
                    [CustomerPurchaseOrderNumber] ,
                    [IsUndersupplyBackordered] ,
                    [Comments] ,
                    [DeliveryInstructions] ,
                    [InternalComments] ,
                    [PickingCompletedWhen] ,
                    [LastEditedBy] ,
                    [LastEditedWhen]
                )

                SELECT [source].[OrderID] ,
                       [source].[CustomerID] ,
                       [source].[SalespersonPersonID] ,
                       [source].[PickedByPersonID] ,
                       [source].[ContactPersonID] ,
                       [source].[BackorderOrderID] ,
                       [source].[OrderDate] ,
                       [source].[ExpectedDeliveryDate] ,
                       [source].[CustomerPurchaseOrderNumber] ,
                       [source].[IsUndersupplyBackordered] ,
                       [source].[Comments] ,
                       [source].[DeliveryInstructions] ,
                       [source].[InternalComments] ,
                       [source].[PickingCompletedWhen] ,
                       [source].[LastEditedBy] ,
                       [source].[LastEditedWhen]
                FROM [#Orders] AS [source]
                LEFT JOIN [Sales].[Orders]  AS [target]
                    ON [target].[OrderID] = [source].[OrderID]
                WHERE [target].[OrderID] IS NULL;
        END
        ELSE IF (@operation = 'D')
        BEGIN
            DELETE [source]
            FROM [Sales].[Orders]  AS [source]
            JOIN [#Orders] AS [target]
                ON [source].[OrderID] = [target].[OrderID];
        END
    END TRY
    BEGIN CATCH
        RETURN -1;
    END CATCH

    RETURN 0;
END
GO

CREATE PROCEDURE [repl].[ProcessSchemaSyncMessage](
    @message XML
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PublisherID UNIQUEIDENTIFIER = (
        SELECT @message.value('(/Tables/@PublisherID)[1]', 'uniqueidentifier')
    );
    BEGIN TRY
    
        BEGIN TRANSACTION;
            DELETE [repl].[SubscribedColumns]
            WHERE [PublisherID] = @PublisherID;

            INSERT INTO [repl].[SubscribedColumns]
            (
                [PublisherID] ,
                [ObjectName] ,
                [ColumnName] ,
                [ColumnID]
            )
        
            SELECT @PublisherID,
                tn.value('(./@name)[1]', 'sysname'),
                cn.value('(./@name)[1]', 'sysname'),
                cn.value('(./@id)[1]', 'int')
            FROM @message.nodes('Tables/Table') AS t(tn)
            CROSS APPLY tn.nodes('Columns/Column') AS c(cn);
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF (XACT_STATE() = -1)
            ROLLBACK;
    END CATCH
END
GO

CREATE PROCEDURE [repl].[SubscriberActivation]
AS
BEGIN
    DECLARE
        @message_type nvarchar(256),
        @message VARBINARY(MAX),
        @rc INT;

    DECLARE @messages TABLE (
        [message_type] sysname, 
        [message] VARBINARY(MAX)
    );
    SET NOCOUNT ON;

    WHILE(1=1)
    BEGIN

        WAITFOR(
            RECEIVE TOP (1000)
                [message_type_name],
                [message_body]
            FROM [repl].[ReplicationQueue]
            INTO @messages
        ), TIMEOUT 5000;

        IF (@@rowCount = 0)
            BREAK;
        ELSE
        BEGIN

            DECLARE [messages] CURSOR FAST_FORWARD LOCAL
            FOR
            SELECT [message], [message_type]
            FROM @messages;

            OPEN [messages];
            WHILE(1=1)
            BEGIN
                FETCH NEXT FROM [messages] 
                INTO @message, @message_type;
                IF (@@fetch_Status <> 0)
                    BREAK;

                BEGIN TRY

                    IF (@message_type IN ('InvoiceInsert', 'InvoiceUpdate', 'InvoiceDelete'))
                    BEGIN
                        EXEC @rc = [repl].[ProcessInvoiceMessage] @message = @message;
                    END
                    ELSE IF (@message_type IN ('OrderInsert', 'OrderUpdate', 'OrderDelete'))
                    BEGIN
                        EXEC @rc = [repl].[ProcessOrderMessage] @message = @message;
                    END
                    ELSE IF (@message_type = 'SchemaSync')
                    BEGIN
                        EXEC @rc = [repl].[ProcessSchemaSyncMessage] @message = @message;
                    END

                    IF (@rc <> 0)
                    BEGIN
                        INSERT INTO [repl].[DeadLetters]
                                ( [Payload], [MessageType] )
                        VALUES  ( @message, @message_type );
                    END
                END TRY
                BEGIN CATCH
                    INSERT INTO [repl].[DeadLetters]
                            ( [Payload], [MessageType] )
                    VALUES  ( @message, @message_type );
                END CATCH
            END
            CLOSE [messages];
            DEALLOCATE [messages];

            DELETE @messages;
        END
    END
END
GO

ALTER QUEUE [repl].[ReplicationQueue]
    WITH ACTIVATION (
        PROCEDURE_NAME = [repl].[SubscriberActivation], 
        MAX_QUEUE_READERS = 5, 
        STATUS = ON,
        EXECUTE AS SELF
);