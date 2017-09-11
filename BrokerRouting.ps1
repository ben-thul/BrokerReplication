param (
    [string]$server1,
    [string]$database1,
    [string]$service1,

    [string]$server2,
    [string]$database2,
    [string]$service2
)

if ($server1 -eq $server2 -and $database1 -eq $database2) {
    'The specified databases are the same. Exiting';
    exit;
}

push-location;
import-module sqlps -disablenamechecking;
pop-location;

$s1 = new-object microsoft.sqlserver.management.smo.server $server1;
$s2 = new-object microsoft.sqlserver.management.smo.server $server2;

$services = @(
    @{
        server = $s1;
        database = $s1.databases[$database1];
        service = $service1;
    },
    @{
        server = $s2;
        database = $s2.databases[$database2];
        service = $service2;
    }
)


$addr = 'TCP://{0}:4022';
$route_name_template = "{0}_{1}";

$affected_routes = 0

while($affected_routes -lt 2) {
    ( $source_server,         $source_database,         $source_service) = 
    ( $services[0]["server"], $services[0]["database"], $services[0]["service"]);

    ($destination_server,     $destination_database,    $destination_service) = 
    ( $services[1]["server"], $services[1]["database"], $services[1]["service"] );

    $broker = $source_database.ServiceBroker;
    $route_name = $($route_name_template -f $destination_database.Name, $destination_service);
    $route = $broker.Routes | where { $_.Name -eq $route_name }

    if ($route -eq $null) {
        $route = new-object microsoft.sqlserver.management.smo.Broker.ServiceRoute $broker, $route_name;
    }

    if ($source_server.Name -eq $destination_server.Name) {
        $route.Address = 'LOCAL';
    }
    else {
        $route.Address = $addr -f $destination_server.Name
    }
    $route.BrokerInstance = $destination_database.ServiceBrokerGUID;
    $route.RemoteService = $destination_service;

    if ($route.State -eq 'Existing') {
        $route.Alter();
        write-debug -message $("Route for service {0} has been altered in {1}." -f $destination_service, $source_database);
    }
    else {
        $route.Create();
        write-debug -message $("Route for service {0} has been created in {1}." -f $destination_service, $source_database);
    }

    $affected_routes += 1;

    # swap the two elements in the array so we
    # can get the route going the other way

    ($services[0], $services[1]) = ($services[1], $services[0]);
}
