#!/bin/sh

set -o errexit
set -o nounset

echo ". . . . . Faust Transactions Is RUNNING! . . . . ."
faust -A streams.transaction worker -l info -p 6066

#echo ". . . . . Faust Producer Is RUNNING! . . . . ."
#faust -A streams.producer worker -l info -p 7001
