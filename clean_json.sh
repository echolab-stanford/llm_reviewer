#!/bin/bash

# Unnest JSON data to be records only and avoid reading issues in DuckDB. The 
# script will take data from a folder defined in the first argument and then
# save it as a cleaned version in the directory. 


DIR_READ=$2
DIR_SAVE=$3
N_JOBS=$4

# Create directory if it doesn't exist
if [ ! -d "${DIR_SAVE}" ]; then
    mkdir -p "${DIR_SAVE}"
    chmod u+w "${DIR_SAVE}"
fi

function help_menu () {
cat << EOF

Usage: ${0} {clean|purge}
OPTIONS:
   -h|help             Show this message
   clean
   purge
EXAMPLES:
   Clean the JSON files in the current directory
        $ ./clean_json clean . ./save_dir
   Destroy all old files:
        $ ./clean_json purge .
EOF
}


function clean () {
        # Find a replace all files and prepend the cleaned_
        cd ${DIR_READ}
        find . -name "*.json.gz" |
        parallel -j $N_JOBS 'gzip -dc {} | jq ".items" | gzip > cleaned_{/.}.gz'
}

function purge () {
        find ${DIR_READ} -name "*.json.gz" -delete
}

function all () {
        clean
        purge
}


if [[ $# -eq 0 ]] ; then
	help_menu
	exit 0
fi

case "$1" in
    clean)
            clean
		shift
        ;;
    purge)
            purge
		shift
        ;;
    all)
        all
                shift
       ;;
    -h|--help)
        help_menu
                shift
        ;;
   *)
       echo "${1} is not a valid flag, try running: ${0} --help"
	   shift
       ;;
esac
shift

