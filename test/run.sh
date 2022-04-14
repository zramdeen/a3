#!/bin/bash

# clean before running
./clean.sh

# load & compile
cp ../src/* .
javac *.java

# message
printf "files copied and compiled\n"
