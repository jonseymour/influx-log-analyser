#!/bin/bash

set -o pipefail

prepare() {
	csv-to-json \
	| jq '.url.query.stem=(if .url.query.q then .url.query.q|split(" where")[0]|split(" WHERE")[0] else "" end)' \
	| jq '.timeScale=(if .duration < 1000 then "ms" else ( if .duration < 60000 then "s" else "m" end ) end)' \
	| json-to-csv --columns=duration,requestId,startedAt,status,url.query.q,url.path,url.query.stem,timeScale \
	| surrogate-keys --natural-key=url.query.stem --surrogate-key=url.query.stem.hash
}

convert() {
	influx-line-format \
		--measurement httpd \
		--tags=status,url.path,url.query.stem.hash,timeScale \
		--timestamp startedAt \
		--format="2006-01-02 15:04:05.999" \
		--values=requestId,duration,url.query.q,url.query.stem
}

write() {
	local url=$1
	(
		while true; do
			count=0
			while test $count -lt 1000; do
				read -r line
				if test -z "$line"; then
					exit 1
				fi
				let count=count+1
				echo "$line"
			done | curl --data-binary @- -XPOST "$url" || exit 0
		done
	) || exit $?
}

prepare-convert-and-write() {

	url="${1:-http://localhost:8086/write?db=httpdlogs&precision=ns}"

	prepare | convert | write "$url"
}

"$@"