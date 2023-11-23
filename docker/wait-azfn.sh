#!/bin/bash
#
# Check if azfn container is healthy
#
set -e

container_name=docker-azure-function-host-1

echo "# Test ${APP}-$container_name up"
set +e
timeout=120;
statuscode=""
until [ "$timeout" -le 0 -o "$statuscode" == "200" ] ; do
	statuscode=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' localhost:8080)
	echo "Wait $timeout seconds: $container_name up $statuscode";
	(( timeout-- ))
	sleep 1
done

if [ "$statuscode" == "200" ]; then
   echo "$container_name in $statuscode state."
else
	echo "ERROR: $container_name in $statuscode state."
	exit 1
fi

exit 0