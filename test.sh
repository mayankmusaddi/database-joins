#!/bin/bash
./20171115.sh Generator/inputR Generator/inputS "$1" "$2"
sort inputR_inputS_join.txt > Join-Validator/mout
diff Join-Validator/mout Join-Validator/outputFile
echo "Validated"
rm inputR_inputS_join.txt