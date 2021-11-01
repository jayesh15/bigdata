#!/bin/bash
cd /home/ak/Desktop/Project/schema1/
sed -i 's/\r$//' *
for f in *.csv
do
	sudo mysql -e "use covid; load data local infile '"$f"' into table covid1 fields TERMINATED BY ',' ENCLOSED BY '\"'LINES TERMINATED BY '\n' ignore 1 lines"  -u root --password=root --local-infile
done

cd /home/ak/Desktop/Project/schema2/
sed -i 's/\r$//' *
for f in *.csv
do
	sudo mysql -e "use covid; load data local infile '"$f"' into table covid2 fields TERMINATED BY ',' ENCLOSED BY '\"'LINES TERMINATED BY '\n' ignore 1 lines"  -u root --password=root --local-infile
done

cd /home/ak/Desktop/Project/schema3/
sed -i 's/\r$//' *
for f in *.csv
do
	sudo mysql -e "use covid; load data local infile '"$f"' into table covid3 fields TERMINATED BY ',' ENCLOSED BY '\"'LINES TERMINATED BY '\n' ignore 1 lines"  -u root --password=root --local-infile
done

cd /home/ak/Desktop/Project/schema4/
sed -i 's/\r$//' *
for f in *.csv
do
	sudo mysql -e "use covid; load data local infile '"$f"' into table covid4 fields TERMINATED BY ',' ENCLOSED BY '\"'LINES TERMINATED BY '\n' ignore 1 lines"  -u root --password=root --local-infile
done
