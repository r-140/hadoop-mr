# Hadoop MapReduce Average Departure Delay Application
upload csv files to google storage bucket
1. open GCP console and login to dataproc cluster
   gcloud compute ssh procamp-cluster-m --zone=us-east1-b --project=<project_id>
2. clone the project from git


To upload sample data:
open upload_from_local_to_hdfs file and change STORAGE_PATH to your bucket
 - run `./upload_from_local_to_hdfs.sh -f txt` to upload csv files from storage bucket to `bdpc/Avg_Delays_MR/Avg_delays/input`
 
To run:
 - Check out script parameters with `./submit_avg_delay.sh -h` or open the file 
 - if it is not executable run `chmod +x submit_avg_delay.sh`
 