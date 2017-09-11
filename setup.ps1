$s = 'localhost'
$pubs = @('WorldWideImporters');
$subs = @('WorldWideImportersSub1', 'WorldWideImportersSub2');

$DataFilePath = 'c:\temp\SQLDATA';
$BackupPath = "$home\Downloads";

foreach ($db in $pubs + $subs) {
    write-debug -message $db;
    foreach ($script in $(get-childitem sql\all\*.sql)) {
        write-debug -message $(" "*4 + "$script");

        $vars = @("DBName=$db", "BackupPath=$BackupPath", "DataFilePath=$DataFilePath");
        invoke-sqlcmd -Server $s -InputFile $script.FullName -Variable $vars
    }   
}

foreach ($sub in $subs) {
    write-debug -message $sub;

    foreach ($script in $(get-childitem sql\sub\*.sql)) {
        write-debug -message $(" "*4 + "$script");

        $vars = @("DBName=$sub"); 
        invoke-sqlcmd -Server $s -InputFile $script.FullName -Variable $vars
    }

    foreach ($pub in $pubs) {
        .\BrokerRouting -server1 . -database1 $pub -service1 ReplicationService -server2 . -database2 $sub -service2 ReplicationService
    }
}

foreach ($pub in $pubs) {
    write-debug -message $pub;
    foreach ($script in $(get-childitem sql\pub\*.sql)) {
        write-debug -message $(" "*4 + "$script");

        $vars = @("DBName=$pub"); 
        invoke-sqlcmd -Server $s -InputFile $script.FullName -Variable $vars
    }
}
