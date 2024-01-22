# TrailcamScripts
Scripts used to process data files to manage, update trailcam data

Usage: 

1) setup a conda environment with the gcloud suite
2) Configure the config file. You will need to set your own project, as well as download a key for a service account with the right permissions. That means you need at a minimum:
```
BigQuery Data Editor
BigQuery Job User
```
4) Run the script on the data files

```
(gcloud) henrik@paris:~/Wildlife$ python refreshBQ.py 
Loaded 1263 rows and 13 columns to my-project-1234.oklahoma_trailcam.oklahoma_trailcam_observations
Loaded 1257 rows and 3 columns to my-project-1234.oklahoma_trailcam.oklahoma_trailcam_deer
(gcloud) henrik@paris:~/Wildlife$
```

At this point you should be able to query the BigQuery tables using the console.
