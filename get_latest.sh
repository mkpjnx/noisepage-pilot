rp=$(realpath $1)
find $rp/*summary* -maxdepth 1 -type f -exec stat -c '%X %n' {} \; | sort -nr | awk  --field-separator=' ' '{print $2}' | head -n 1
