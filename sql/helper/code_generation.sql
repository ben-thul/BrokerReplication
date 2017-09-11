DECLARE @table sysname = 'Sales.Orders'
WITH cte AS (
    SELECT name,
        QUOTENAME(name) AS qname,
        TYPE_NAME([c].[system_type_id]) AS t,
        CASE WHEN TYPE_NAME([c].[system_type_id]) LIKE '%varchar'
            THEN CONCAT('VARBINARY(', CASE WHEN [c].[max_length] = -1 THEN 'MAX' ELSE CAST([c].[max_length] AS VARCHAR(10)) END, ')')
            ELSE TYPE_NAME([c].[system_type_id])
        END AS [derived_type],
        [c].[max_length]
    FROM sys.[columns] AS [c]
    WHERE OBJECT_ID = OBJECT_ID(@table)
        AND COLUMNPROPERTY(OBJECT_ID(@table), [c].[name], 'IsComputed') = 0
)
SELECT CONCAT(
    QUOTENAME('@' + name), ' = ',
    'CASE WHEN UPDATE(', qname, ') ',
    'THEN ', 
    
    CASE WHEN [derived_type] <> [cte].[t] THEN CONCAT('CAST([i].', [cte].[qname], ' as ', [cte].[derived_type], ')')
         ELSE  CONCAT('[i].', [cte].[qname])
    END, ' ELSE NULL END ,'
) AS [trigger], 

    CONCAT(
        'n.value(''(./@', [cte].[name], ')[1]'', ''', [cte].[derived_type], ''') as ', [cte].[qname], ' ,'
    ) AS [xm_shred],
    CONCAT(
        [cte].[qname], ' = CASE WHEN [updated_columns].', [cte].[qname], 
        ' = 1 THEN [source].', [cte].[qname], 
        ' ELSE [target].', [cte].[qname], ' END ,'
    ) AS [activation_update],
    CONCAT([cte].[qname], ' ,') AS [qname]
FROM cte