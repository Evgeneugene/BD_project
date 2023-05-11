hdfs dfs -rm -r /project/avsc/
hdfs dfs -mkdir /project/avsc
hdfs dfs -put *.avsc /project/avsc

rm -r output/
hive -f sql/db.hql > output/hive_results.txt

hive -f sql/query1.hql
echo "asset_name, train_count" > output/q1.csv
cat output/q1/* >> output/q1.csv

hive -f sql/query2.hql
echo "asset_name, avg_vwap" > output/q2.csv
cat output/q2/* >> output/q2.csv

hive -f sql/query3.hql
echo "asset_name, count" > output/q3.csv
cat output/q3/* >> output/q3.csv

hive -f sql/query4.hql
echo "time, open, high, low, close" > output/q4.csv
cat output/q4/* >> output/q4.csv

hive -f sql/query5.hql
echo "time, open, close" > output/q5.csv
cat output/q5/* >> output/q5.csv
