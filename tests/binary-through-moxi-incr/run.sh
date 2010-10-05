#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    KEYFILE

RETVAL=0

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		echo "[$TESTNAME] Running incr against $entry"
		# do the set first so we know we have valid data to incr.
		bin/binclient.py $entry set a 1
		OUTPUT=`bin/binclient.py $entry incr a 1`
		if [ $? -eq 1 ] || [ "$OUTPUT" -ne "2" ]; then
			echo "[$TESTNAME] got unexpected output: $OUTPUT (expected: 2)"
			RETVAL=1
		fi	
	done
else
	echo "[$TESTNAME] Running incr against $SERVER"
	# do the set first so we know we have valid data to incr.
	bin/binclient.py $SERVER set a 1
	OUTPUT=`bin/binclient.py $SERVER incr a 1`
	if [ $? -eq 1 ] || [ "$OUTPUT" -ne "2" ]; then
		echo "[$TESTNAME] got unexpected output: $OUTPUT (expected: 2)"
		RETVAL=1
	fi	
fi

exit $RETVAL