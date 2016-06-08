#! /bin/bash
rm conf/workers.conf
cp ./worker_files/one_worker.conf ./conf/workers.conf
SELECTION_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/selection_query.txt | grep "seconds")"
JOIN_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/join_query.txt |grep "seconds")"
AGG_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/agg_query.txt | grep "seconds")"

echo "=== One Worker ==="
echo "Selection Query Time: $SELECTION_TIME"
echo "Join Query Time: $JOIN_TIME"
echo "Aggerate Query Time: $AGG_TIME"

rm conf/workers.conf
cp ./worker_files/two_worker.conf ./conf/workers.conf
SELECTION_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/selection_query.txt | grep "seconds")"
JOIN_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/join_query.txt |grep "seconds")"
AGG_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/agg_query.txt | grep "seconds")"

echo "=== Two Workers ==="
echo "Selection Query Time: $SELECTION_TIME"
echo "Join Query Time: $JOIN_TIME"
echo "Aggerate Query Time: $AGG_TIME"

rm conf/workers.conf
cp ./worker_files/four_worker.conf ./conf/workers.conf
SELECTION_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/selection_query.txt | grep "seconds")"
JOIN_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/join_query.txt |grep "seconds")"
AGG_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/agg_query.txt | grep "seconds")"

echo "=== Four Workers ==="
echo "Selection Query Time: $SELECTION_TIME"
echo "Join Query Time: $JOIN_TIME"
echo "Aggerate Query Time: $AGG_TIME"

rm conf/workers.conf
cp ./worker_files/eight_worker.conf ./conf/workers.conf
SELECTION_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/selection_query.txt | grep "seconds")"
JOIN_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/join_query.txt |grep "seconds")"
AGG_TIME="$(./bin/startSimpleDB.sh etc/imdb.schema -f ./queries/agg_query.txt | grep "seconds")"

echo "=== Eight Workers ==="
echo "Selection Query Time: $SELECTION_TIME"
echo "Join Query Time: $JOIN_TIME"
echo "Aggerate Query Time: $AGG_TIME"
