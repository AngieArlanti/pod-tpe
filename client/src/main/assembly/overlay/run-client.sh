#!/bin/bash

java -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client" -Dname '53373-53248-52015-53852' -Dpass cluster-pass "$@"