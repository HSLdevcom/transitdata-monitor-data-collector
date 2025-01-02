#!/bin/bash
if ! command -v jq &> /dev/null
then
    echo "jq could not be found, installing..."
    apt-get update && apt-get install -y jq
fi

if ! command -v curl &> /dev/null
then
    echo "curl could not be found, installing..."
    apt-get update && apt-get install -y curl
fi

if [ -e .env ]
then
    export $(grep -v '^#' .env | xargs)
fi

outputNamespaces=$(curl $ADMIN_URL/admin/v2/namespaces/$NAMESPACE 2>/dev/null)

echo "$outputNamespaces" | jq -r '.[]' | while read -r i; 
do 
        outputTopics=$(curl $ADMIN_URL/admin/v2/persistent/$i 2>/dev/null)

        echo "$outputTopics" | jq -r '.[]' | while read -r j;
        do
                outputStats=$(curl $ADMIN_URL/admin/v2/persistent${j:12}/stats 2>/dev/null)
                backlogSize=$(echo $outputStats | jq '.backlogSize')

                if [ ${#backlogSize} -gt 9 ]
                then
                        echo "BacklogSize: ${backlogSize:0:${#backlogSize}-9}G. TOPIC: $j"
                fi
        done
done