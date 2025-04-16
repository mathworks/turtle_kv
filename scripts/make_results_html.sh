#!/bin/bash
#
set -Eeuo pipefail

script_dir=$(cd "$(dirname "$0")" && realpath .)

cd "${script_dir}/../build/benchmark_results"

html_file=$(pwd)/results.html

{

    echo '<!DOCTYPE html>'
    echo '<html lang="en" data-content_root="../../" >'
    echo '  <head>'
    echo '    <meta charset="utf-8" />'
    echo '  </head>'
    echo '  <body>'

    prev_log=null
    for img in $(find . -type f -name '*.png' | sort -h); do
        this_log=$(basename $img | sed -e 's,\., ,g' | awk '{print $1}')
        if [ "${this_log}" != "${prev_log}" ]; then
            prev_log=${this_log}
            echo "<hr>${this_log}<br>"
        fi
        echo "<img src='${img}' width='24%'/>"
    done
    
    echo '  </body>'
    echo '</html>'
    
} > "${html_file}"

echo "Wrote ${html_file}"
