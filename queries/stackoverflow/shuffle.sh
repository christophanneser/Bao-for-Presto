rm stack_queries.txt

for dir in $(ls); do
    queries=$(ls $dir | shuf | grep -v .sql-E | grep -v schema | grep .sql | head -n 25);
    for q in $queries; do
        echo $dir/$q >> stack_queries.txt;
    done;
done;

