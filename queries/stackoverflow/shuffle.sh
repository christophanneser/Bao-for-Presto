rm result.txt;

for dir in $(ls); do
    queries=$(ls $dir | shuf | grep -v .sql-E | head -n 6);
    for q in $queries; do
        echo $dir/$q >> result.txt;
    done;
done;
