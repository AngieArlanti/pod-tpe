#!/bin/bash

java -cp 'lib/jars/*' -Dname='53373' -Dpass=cluster-pass "$@" "ar.edu.itba.pod.client.Client"