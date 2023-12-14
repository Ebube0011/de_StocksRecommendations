# Stocks Financial Data ETL
![Template Arch](https://github.com/Ebube0011/de_StocksRecommendations/assets/149321069/27b2c1de-edc7-4ba8-b1b8-0562f1645ac5)
This is my first Data Engineering project to extract data from API for stocks recommendations to be done on MS Excel using VBA. It's a simple project designed to help me practice using containers (docker), orchestration (airflow), api's, improving my programming and data transformation skills

## Description
The project extracts stockds data from yahoo finance api, transforms it, and loads it into Amazon S3 where it can then be downloaded and used with MS Excel for stock Ananlytics. Python was used as the programming language as it is the only programming language i know, for now. It is used for the extraction, transforamtion, and loading of the data. Airflow is used for orchestration, although this can done using simple cronjobs. Airflow obviously isn't crucial for this project but this served as a good enough excuse to practice it.
The final data is loaded into Amazon S3 bucket, this also isn't necessary. The data could have also been loaded into a relational database like mysql, but for the sake of simplicity I decided to use an object storage. 

## Project stages
- Ingestion
- Transformation
- Serving

### Ingestion
At this stage we need to extract the necessary data ingredients required for the project, but the tools are still needed to be setup to ensure smooth operations moving forward. In this stage, the main tools to be setup are:
- the linux computer
  I'll assume that our linux computer (or compute instance) is running and ready to go. Docker and Airflow do have certain minimum system requirements to be able to run smoothly, so please do keep that in mind.
- Amazon S3 bucket
  I will also assume that we have an account on AWS and can setup a bucket for this project.
- Docker
  The next step will be to install docker. There are a few ways to do so, the unofficial and official. If you choose to go ahead with the unofficial method, it wouldn't be hard at all. All you would to do is run the following commands on your linux terminal
```bash
sudo apt install docker.io
sudo apt install docker-compose
```
If you however choose to use the official version of docker, please follow the instructions in this link: https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository
- Airflow
  Having installed docker, run this command to clone a repository with the special tools to run airflow on docker. We could have built the docker file and image ourselves but that is something i'm still learning
```bash
git clone https://github.com/dogukannulu/docker-airflow.git
```
Next, install the necessary requirements using this command:
```bash
docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
```
Then alter the docker-compose-LocalExecutor.yml file contensts with the one in this repo, add requirements.txt and yahoo_api.env file in the folder. You can do this by simply using the command
```bash
sudo nano docker-compose-LocalExecutor.yml    # delete the contents and replace with contents of the one in this repository
sudo nano requirements.txt    # copy and paste from the contents of the one in this repository
sudo nano yahoo_api.env    # insert the details of the required environment variables
```
Now it's time to make the magic happen, it's time to run the containers for this stage using docker compose.
```bash
docker compose -f docker-compose-LocalExecutor.yml up -d
```
Now we have a running Airflow container and you can access the UI at https://localhost:8080
We should now move/create the Ingestion.py and Ingestion_DAG.py scripts from this repository to the dags folder in docker-airflow repo. Then we can see that Stocks_ETL appears in DAGS page.
When we turn the OFF button to ON, we can see that the data will be sent to Amazon S3 bucket prepared.
To shut down the containers, simply use the command
```bash
docker compose -f docker_something.yml down
```
 
### Serving
At this stage we simply download the data from the S3 bucket into the local computer for data analytics on the data. 

Enjoy!

