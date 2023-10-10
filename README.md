# Hadoop MapReduce Word Counter Application example

To upload samle data:
 - run `./upload_from_local_to_hdfs.sh -f txt` to upload `../data/word_count` to `/bdpc/hadoop_mr/word_count/input`
 - run `./upload_from_local_to_hdfs.sh -f gz` to upload `../data/word_count_gzip` to `/bdpc/hadoop_mr/word_count/input`

To run:
 - Check out script parameters with `./submit_word_counter.sh -h` or open the file 
 - It can be executed with default parameters
  > ./submit_failing.sh
 - if it is not executable run `chmod +x submit_word_counter.sh`
 