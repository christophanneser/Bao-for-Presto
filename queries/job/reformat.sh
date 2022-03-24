#! /bin/bash

for file in $(ls e*sql); do
    echo $file
    rm .tmp.sql
    sqlformat --strip-comments  -r -k upper $file >> .tmp.sql
    mv .tmp.sql $file
done;
