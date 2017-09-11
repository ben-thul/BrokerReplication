USE master;
IF DB_ID('$(DBName)') IS NOT NULL
BEGIN
    ALTER DATABASE [$(DBName)] SET OFFLINE WITH ROLLBACK IMMEDIATE;
    ALTER DATABASE [$(DBName)] SET ONLINE;
    DROP DATABASE [$(DBName)];
END
GO
EXECUTE AS LOGIN = 'sa';
GO

RESTORE DATABASE [$(DBName)]
    FROM DISK = '$(BackupPath)\WideWorldImporters-Full.bak'
    WITH STATS = 5 ,
         MOVE 'WWI_Primary'
             TO '$(DataFilePath)\$(DBName)\$(DBName).mdf' ,
         MOVE 'WWI_UserData'
             TO '$(DataFilePath)\$(DBName)\$(DBName)_UserData.ndf' ,
         MOVE 'WWI_Log'
             TO '$(DataFilePath)\$(DBName)\$(DBName).ldf' ,
         MOVE 'WWI_InMemory_Data_1'
             TO '$(DataFilePath)\$(DBName)\$(DBName)_InMemory_Data_1';
GO

USE [$(DBName)];
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'AReallyStrongPassword!23'
ALTER MASTER KEY ADD ENCRYPTION BY SERVICE MASTER KEY;
-- create a certificate for transport encryption
CREATE USER [ReplUser] WITHOUT LOGIN;

CREATE CERTIFICATE [ReplicationCertificate] AUTHORIZATION [ReplUser] FROM BINARY = 0x308202E4308201CCA00302010202105DF2C671E8911B9E401D1BBF45137DA3300D06092A864886F70D0101050500302D312B302906035504031322436572746966696361746520666F722042726F6B6572205265706C69636174696F6E3020170D3137303930353030313235355A180F32303939303130313030303030305A302D312B302906035504031322436572746966696361746520666F722042726F6B6572205265706C69636174696F6E30820122300D06092A864886F70D01010105000382010F003082010A0282010100BED1BAA2B9BE41A24DA17CA71AFB156C3E7584A27375AA9E3EB5E440D298812300BBD5CBB696C0C6ABC3137AD2EB6EB1642416ED39BDC7EB36433E3B361597AEE296420FE3E240C261EED1A13B109AF323C0C65B9C7AFA2FAD8E7CB2D70258DF88C894A7EF47FF607D5E7CF994F0680A30CEF2900C327BF73F59D0708A83BCFF6C2DA52BF7F314DBD5842092FC1C130FE6666C3B39E19AC9C54D731D38A8760F625A857D0ED78D1F4D016BE4B8549C5EA3D478073FD961AA6622EBA1A1E9DCE20694484FA432A9FD5B6EF958ADB899D5D7C9CC0A938FFF18501F6B06CC19FD51EB0A16812D3BE7B57E2949D53387B22E28F0FE70D2EA42B942321CD28CD6FF9D0203010001300D06092A864886F70D010105050003820101007C64E6393CA722BEE6AAEC27A6DB3CC1F797F159F68882CA995285CF0739A8E19F4CA066102215C698FE0F248D22D39358BD5A37F6F4DA1E01C9B23EEADE837441583E85702871B7E951BD841924F16F0B81CF96D9F0FAD69F742363735C85D493B9FE78358C7B1E96557620FFB490754E12247B65E191E285072170992F12EAF6ECF47D0C1D13EF225DF4B9B2B96630384B1889A317A0FA6F708444970B1CB31174756B01E9FB695B8555B1CBB35E767124A032CBDF00413D445228B4DAD5EA2AAAB02A498CA18A9E9DB4E4F25212E655809055D12B104AEB669576D1842B8555BA41826FACC29790B227975E2C7ECE7CA0C206023D683E2B80A5FE0786B5DC WITH PRIVATE KEY (BINARY = 0x1EF1B5B00000000001000000010000001000000094040000A19B73AB1606F54EF9682AFE2728850D0702000000A40000590B63A4DF120A9EF130932ECACEC6131D4FCFB67737039C0D97016716AB079E5949C1B07673168D73A8D06A4B0AFA942EB768AD07F449B25B4EEBEE2C3C1434AF356A442BB62863AD39711CD5DA8B58277D00E2787323F59C76DC8C6B3D8E0482A3466A0DFF401A89DD6C1899F8DB856FBD97264AB71C7ECD2DD55FDBA1819EC10DE91C546C6289C8A51440EA14D48342E750BF0E6DB36C4AAC6E94BC8AE014DFFACCF989B5A2B37E7DAD7113225EE1250D2F0F10560E9EFC2FCB380E17A122430952B6D9384F10D504B30A225F141B0605108970984DDD024FDF686BE35FEA698693B522F9BDA031133BF855295C2D3A9320E897209E49514723121CA167EAC551AF7B3DD43BC537BF094A0642FD88FF1BCE6873F24C9A9FFF5895F3CBA365F136C908E9298DB7E51570D92ED49B5C5DF9996824D6330BF15982C034F69AEB7794E4E43849EF1AF1C1E1B27B6E293029218B7D8318BBC9FEF2D2FC4FFBF403AC121B2F7B5C67162D8C3B47AC43513A58D48B19C8EB33467B7689D4BBB86279164C0074A7CDA55369E3E099E6E9176C855DC86DF8F4C2A6D55BDD65EF69CFA3EA7A041DEE48D8CA2A058B9AF6EF85FAA1B837E40D799940B73859222111E19443636E68C11915E101326EC828D5F66B6B42DFA0F746B9D03C3FCCC6402250E4EE3ACCAA683FB0FA2D14F4A9B923CCE05578684B81ACF13D3DB315821B5C25A89854C5DBD1550BBB3F74BAD52F915FD68455CF3610DFF038E9AA3E1FB39018D907953C26B218CE64E6DE390DB0AED55BB5E99A11A12C5D59B3FA425D9CD389693CE28DE452FFE00401FC8266681CDE270357D1E1CA68BFDBC52D8B27619890D144634D2FC2E2B345756B1F08CDF8FC990703154C3C00BC4A3652509CAF65B3605389C8AA0D5F7EFF59B170164CC9E5EE2D35704A636DCA5EED1C65D9F14B2348F370FF950A64E7EE3AF59DDA070FF9CFF8485506E1EBF10D2E352D5769825F8C3032CBB87546843A0DC9F1C286742E8B218033D0CB5AF9BE253406E070DBD3162DBB10A2705C70586F258D96A6887E4CF184F70197711412B8BA1CEA1F8C20E71E83830F2898FD8A0D789029CE748A9E5BDDA4595D1738A208DB4E8526BEED957234D6E60669B9FD93E56A2ED10A6BA03BB1E56E98006EA467ADA3383443AF465D285A0560A524BE8088CB340D3EE84382498576E5F95A98B129A232339A5F180BD67AE14C5FB3178892E2E3770033AFE6FD1F23F4AAB24D08F67ECD3F4DB9174004432500E52D2B38B861C8C5E60899FC730474F4DF8798DDFD908F4D6E81101D4EED9A46ADFE6D5FB3264A923A4A4A4D3FC0CEAE598BF51E9158E15988661211D31EB6967138BB5340371987D1D1A355B0FF3195D278116C026BD1D60D46BA8C3FB8BF8FEEC4F017130C972A938A4023DA4EA02449B9EF07C8351775F4BFEE51C15189095B9C8ABE19DC50AA53E331394174635F278528A662A91D83C9B001F15A18ABA13D50ABB517FF9EDAC0321F687D5D949E273EA06D4FD20088E85254BC5CA568D45B369EEFFE0160D92A0F85CB50F7012C8CC2FE0EC76D516319CDCCC23C892D3F1664D37DF0B8D9E786F4D76845C8F5582D17162CAD1634EEDD53D4AE77D2593F2EDAB4BFD5E973, DECRYPTION BY PASSWORD = 'f00bar!23')