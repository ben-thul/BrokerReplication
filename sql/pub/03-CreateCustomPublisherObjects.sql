use [$(DBName)];
GO

CREATE SEQUENCE [repl].[SQ_ConversationGroupID] AS INT START WITH 1;

CREATE TABLE [repl].[StoredConversationGroups] (
    GroupID INT NOT NULL
        CONSTRAINT [PK_StoredConversationGroups] PRIMARY KEY NONCLUSTERED ([GroupID]),
    ContractName sysname NOT NULL,
    ConversationGroupID UNIQUEIDENTIFIER NOT NULL 
);

CREATE CLUSTERED INDEX [CIX_StoredConversationGroups] 
    ON [repl].[StoredConversationGroups] ([ContractName])

CREATE TABLE [repl].[StoredConversations] (
    ConversationHandle UNIQUEIDENTIFIER NOT NULL,
    GroupID INT NOT NULL
        CONSTRAINT [FK_StoredConversations__StoredConversationGroups] 
        FOREIGN KEY ([GroupID])
        REFERENCES [repl].[StoredConversationGroups] ([GroupID])
);

CREATE CLUSTERED INDEX [CIX_StoredConversations] ON [repl].[StoredConversations] ([GroupID])
CREATE SEQUENCE [repl].[SQ_ConversationRing]
    AS INT
    START WITH 0
    INCREMENT BY 1
    MAXVALUE 1000
    CYCLE
    CACHE 20;
GO

CREATE PROCEDURE [repl].[addStoredConversationGroup] (
    @ContractName sysname,
    @count TINYINT = 1,
    @TargetService sysname = 'ReplicationService',
    @SourceService sysname = 'ReplicationService',
    @SubscriberBrokerGUID UNIQUEIDENTIFIER = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @subscribers TABLE (
        [broker_instance] UNIQUEIDENTIFIER NOT NULL
    );

    DECLARE @StoredGroups TABLE (
        GroupID int NOT NULL PRIMARY KEY,
        ConversationGroup UNIQUEIDENTIFIER NOT NULL
    )
    DECLARE @StoredConversations TABLE (
        GroupID INT NOT NULL,
        ConversationHandle UNIQUEIDENTIFIER NOT NULL
    )
    DECLARE @storedCount INT = 0, 
        @actualCount INT = 0,
        @ConversationHandle UNIQUEIDENTIFIER,
        @GroupID INT,
        @ConversationGroup UNIQUEIDENTIFIER,
        @i TINYINT = 0 ,
        @BrokerInstance UNIQUEIDENTIFIER;

    IF (@SubscriberBrokerGUID IS NOT NULL)
    BEGIN
        INSERT INTO @subscribers
            ( [broker_instance] )
        VALUES 
            ( @SubscriberBrokerGUID );
    END
    ELSE
    BEGIN
        INSERT INTO @subscribers
            ( [broker_instance] )
        SELECT [broker_instance]
        FROM [sys].[routes] AS [r]
        WHERE [r].[remote_service_name] = @TargetService;
    END

    WHILE(@storedCount < @count)
    BEGIN
        SELECT @actualCount += 1 ,
            @ConversationGroup = NEWID();

        IF (@actualCount % 150 = 0)
        BEGIN
            SELECT @GroupID = NEXT VALUE FOR [repl].[SQ_ConversationGroupID],
                @storedCount += 1;
            INSERT INTO @StoredGroups
            (
                [GroupID] ,
                [ConversationGroup]
            )
            VALUES
            (   
                @GroupID ,
                @ConversationGroup
            );
        END


        DECLARE [c] CURSOR FAST_FORWARD FOR
        SELECT [s].[broker_instance]
        FROM @subscribers AS [s];

        OPEN [c]

        WHILE(1=1)
        BEGIN
            FETCH NEXT FROM [c] INTO @BrokerInstance;
            IF @@FETCH_STATUS <> 0
                BREAK;

            BEGIN DIALOG CONVERSATION @ConversationHandle
                FROM SERVICE @SourceService
                TO SERVICE @TargetService, @BrokerInstance
                ON CONTRACT @ContractName
                WITH RELATED_CONVERSATION_GROUP = @ConversationGroup;

            IF ( @actualCount % 150 = 0)
            BEGIN
                INSERT INTO @StoredConversations
                (
                    [GroupID] ,
                    [ConversationHandle]
                )
                VALUES
                (   
                    @GroupID ,
                    @ConversationHandle
                )
            END
        END

        CLOSE [c];
        DEALLOCATE [c];

    END

    BEGIN TRAN
        INSERT INTO [repl].[StoredConversationGroups]
        (
            [GroupID] ,
            [ContractName] ,
            [ConversationGroupID]
        )
        SELECT [sg].[GroupID] ,
               @ContractName ,
               [sg].[ConversationGroup]
        FROM   @StoredGroups AS [sg];

        INSERT INTO [repl].[StoredConversations]
        (
            [ConversationHandle] ,
            [GroupID]
        )
        SELECT [sc].[ConversationHandle] ,
               [sc].[GroupID]
        FROM @StoredConversations AS [sc];

    COMMIT TRAN
END
GO

CREATE FUNCTION [repl].[getConversationHandles] (@ContractName sysname, @sequence_value int)
RETURNS @ch TABLE (ConversationHandle UNIQUEIDENTIFIER)
AS
BEGIN
    DECLARE @GroupID INT ,
    @i TINYINT = 1 ,
    @max_attempts TINYINT = 5;

    -- try @max_attempts times to get a conversation handle

    WHILE(@GroupID IS NULL AND @i <= @max_attempts)
    BEGIN
            
        SET @GroupID = (
            SELECT TOP(1) GroupID
            FROM [repl].[StoredConversationGroups] WITH (READPAST, ROWLOCK, XLOCK)
            WHERE [ContractName] = @ContractName
        );

        SET @i += 1;
    END;

    -- we tried (unsuccessfully) @max_attempts times to get a free handle, now
    -- we wait to get an in use one

    IF (@GroupID IS NULL)
    BEGIN
    
        WITH cte AS (
            SELECT [oc].[GroupID], 
                COUNT(*) OVER (PARTITION BY [ContractName]) AS [count],
                ROW_NUMBER() OVER (PARTITION BY [ContractName] ORDER BY [GroupID]) - 1 AS [rn]
            FROM [repl].[StoredConversationGroups] AS [oc]
            WHERE [oc].[ContractName] = @ContractName
        )
        SELECT @GroupID = [cte].[GroupID]
        FROM [cte]
        WHERE @sequence_value % [count] = [rn]

    END

    INSERT INTO @ch
        ( [ConversationHandle] )
    SELECT [sc].[ConversationHandle]
    FROM [repl].[StoredConversations] AS [sc]
    WHERE [sc].[GroupID] = @GroupID

    RETURN
END
GO

CREATE PROCEDURE [repl].[SendMessage] (
    @ContractName sysname,
    @MessageType sysname,
    @message xml
)
AS
BEGIN

    DECLARE 
        @ch_01 UNIQUEIDENTIFIER,
        @ch_02 UNIQUEIDENTIFIER,
        @ch_03 UNIQUEIDENTIFIER,
        @ch_04 UNIQUEIDENTIFIER,
        @ch_05 UNIQUEIDENTIFIER,
        @ch_06 UNIQUEIDENTIFIER,
        @ch_07 UNIQUEIDENTIFIER,
        @ch_08 UNIQUEIDENTIFIER,
        @ch_09 UNIQUEIDENTIFIER,
        @ch_10 UNIQUEIDENTIFIER,
        @send_statement NVARCHAR(MAX),
        @ringValue INT = NEXT VALUE FOR [repl].[SQ_ConversationRing];


    BEGIN TRANSACTION
        
        SELECT @ch_01 = [1] ,
               @ch_02 = [2] ,
               @ch_03 = [3] ,
               @ch_04 = [4] ,
               @ch_05 = [5] ,
               @ch_06 = [6] ,
               @ch_07 = [7] ,
               @ch_08 = [8] ,
               @ch_09 = [9] ,
               @ch_10 = [10]
        FROM (
            SELECT [ConversationHandle] ,
                    ROW_NUMBER() OVER (ORDER BY [ConversationHandle]) AS [rn]
            FROM   [repl].[getConversationHandles]( @ContractName, @ringValue )
        ) AS p
        PIVOT (
            MAX([ConversationHandle]) 
            FOR [rn] IN (
                [1], [2], [3], 
                [4], [5], [6], 
                [7], [8], [9], 
                [10]
            )
        ) AS pvt;

        SET @send_statement = CONCAT(
            'SEND ON CONVERSATION (@ch_01',
            CASE WHEN @ch_02 IS NOT NULL THEN ', @ch_02' END,
            CASE WHEN @ch_03 IS NOT NULL THEN ', @ch_03' END,
            CASE WHEN @ch_03 IS NOT NULL THEN ', @ch_03' END,
            CASE WHEN @ch_04 IS NOT NULL THEN ', @ch_04' END,
            CASE WHEN @ch_05 IS NOT NULL THEN ', @ch_05' END,
            CASE WHEN @ch_06 IS NOT NULL THEN ', @ch_06' END,
            CASE WHEN @ch_07 IS NOT NULL THEN ', @ch_07' END,
            CASE WHEN @ch_08 IS NOT NULL THEN ', @ch_08' END,
            CASE WHEN @ch_09 IS NOT NULL THEN ', @ch_09' END,
            CASE WHEN @ch_10 IS NOT NULL THEN ', @ch_10' END,
            ') MESSAGE TYPE @MessageType ',
            '(@message);'
        );

        EXEC sp_executesql @send_statement,
            N'@ch_01 UNIQUEIDENTIFIER,
             @ch_02 UNIQUEIDENTIFIER,
             @ch_03 UNIQUEIDENTIFIER,
             @ch_04 UNIQUEIDENTIFIER,
             @ch_05 UNIQUEIDENTIFIER,
             @ch_06 UNIQUEIDENTIFIER,
             @ch_07 UNIQUEIDENTIFIER,
             @ch_08 UNIQUEIDENTIFIER,
             @ch_09 UNIQUEIDENTIFIER,
             @ch_10 UNIQUEIDENTIFIER,
             @MessageType sysname,
             @message xml',
            @ch_01 = @ch_01,
            @ch_02 = @ch_02,
            @ch_03 = @ch_03,
            @ch_04 = @ch_04,
            @ch_05 = @ch_05,
            @ch_06 = @ch_06,
            @ch_07 = @ch_07,
            @ch_08 = @ch_08,
            @ch_09 = @ch_09,
            @ch_10 = @ch_10,
            @MessageType = @MessageType,
            @message = @message;
        
    COMMIT TRANSACTION
END
GO

EXEC [repl].[addStoredConversationGroup] 
    @ContractName = 'InsertContract' ,
    @count = 5;

EXEC [repl].[addStoredConversationGroup] 
    @ContractName = 'UpdateContract' ,
    @count = 5;

EXEC [repl].[addStoredConversationGroup] 
    @ContractName = 'DeleteContract' ,
    @count = 5;
GO