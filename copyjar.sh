#!/bin/bash

# This command is ran from the root folder in order to copy your jar file from /target.
# For the most updated jar file you should run 'mvn clean install'. This is a optional.
# You should add 'clean' as an argument running this script to run 'mvn clean install'.
#
# example: copyjar.sh clean

# Optional mvn clean install
cd ..
if [ "$1" = "clean" ]
then
  echo "cleaning up for updated jar file..."
  mvn clean install
fi

# In root directory to parse pom.xml
WORKING_DIRECTORY=`pwd`
PROJECT_NAME=`cat pom.xml | grep "name" | cut -d'>' -f2 | cut -d'<' -f1`
PROJECT_VERSION=`cat pom.xml | grep "SNAPSHOT" | cut -d'>' -f2 | cut -d'<' -f1`

FILE_TO_COPY="${PROJECT_NAME}-${PROJECT_VERSION}".jar

echo "copying jar file: $FILE_TO_COPY to root folder in project $PROJECT_NAME"
cp ./target/"$FILE_TO_COPY" "$WORKING_DIRECTORY"
