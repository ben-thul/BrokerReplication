USE [$(DBName)];
GO

-- Foreign keys for Sales.Invoices
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_SalespersonPersonID_Application_People];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_PackedByPersonID_Application_People];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_Application_People];
ALTER TABLE [Sales].[CustomerTransactions] DROP CONSTRAINT [FK_Sales_CustomerTransactions_InvoiceID_Sales_Invoices];
ALTER TABLE [Sales].[InvoiceLines] DROP CONSTRAINT [FK_Sales_InvoiceLines_InvoiceID_Sales_Invoices];
ALTER TABLE [Warehouse].[StockItemTransactions] DROP CONSTRAINT [FK_Warehouse_StockItemTransactions_InvoiceID_Sales_Invoices];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_CustomerID_Sales_Customers];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_BillToCustomerID_Sales_Customers];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_OrderID_Sales_Orders];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_DeliveryMethodID_Application_DeliveryMethods];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_ContactPersonID_Application_People];
ALTER TABLE [Sales].[Invoices] DROP CONSTRAINT [FK_Sales_Invoices_AccountsPersonID_Application_People];

-- Foreign keys for Sales.Orders
ALTER TABLE [Sales].[OrderLines] DROP CONSTRAINT [FK_Sales_OrderLines_OrderID_Sales_Orders];
ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [FK_Sales_Orders_CustomerID_Sales_Customers];
ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [FK_Sales_Orders_SalespersonPersonID_Application_People];
ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [FK_Sales_Orders_PickedByPersonID_Application_People];
ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [FK_Sales_Orders_ContactPersonID_Application_People];
ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [FK_Sales_Orders_BackorderOrderID_Sales_Orders];
ALTER TABLE [Sales].[Orders] DROP CONSTRAINT [FK_Sales_Orders_Application_People];