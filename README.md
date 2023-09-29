# GCP Uber Data Egineering project

## Table of Contents

- [Introduction](#introduction)
- [Architecture of the Data Pipeline](#architecture-of-the-data-pipeline)
- [Code and Services utilized](#code-and-services-utilized)
- [Data Transformation and Modeling](#data-transformation-and-modeling)
- [Data Storage and VM configuration](#data-storage-and-vm-configuration)
- [ETL Orchestration](#etl-orchestration)
- [Data Analysis](#data-analysis)
- [Dashboarding](#dashboarding)


## Introduction

The main purpose of this project is to detail the core stages of an end-to-end pipeline Data Engineering project. Therefore, many of the aspects regarding Business Intelligence, Analytics and Dashboarding will be out of the scope for this project.

The structure of the architecture of this end-to-end data pipeline is detailed in the following section. 

## Architecture of the Data Pipeline

![image](https://github.com/GBlanch/Data-Engineering/assets/136500426/9133fc2a-a684-4d97-9260-8792304b2838)

The diagram above primarily shows the required stages once the raw data is gathered.

The csv file can be obtained under request from [NYC Taxi & Limousine Commission website](https://www.nyc.gov/site/tlc/about/request-data.page). We could upload at the same time this flat file into Google Cloud Storage, but we rather do it once we have all our code ready to be deployed (we will be needing to configure our Google Cloud Platform settings further on)

Using the data catalogue provided in the website above - and prior to doing anything else -  we need to understand and design the data model structure to be adhered to within this project.
Once the data modeling is done, we will perform the cleaning and transformation coding required to attain the desired data model. This stage will be performed using Python within Jupyter Notebooks, and it will all run be in our LM.

After this, we can store our data flat file in our Cloud Storage and also start setting up all the Cloud services to be used. 

Next, we will be ready to deploy this code into our open-source data pipeline tool Mage. After we’ve created the main ETL batch pipeline in Mage, we will orchestrate it using our IaaS Computer Engine (VM). This will allow us to export our dataframes into our data warehouse in BigQuery.

Once the data is exported, we can start querying the parameters and variables we want to display into our reports/dashboards further on.

Finally, Looker Studio will allow us to create some dashboards according to the queries we have prepared before.

[Back to Table of Contents](#table-of-contents)

## Code and Services utilized

### Coding:

We will use `Python` within Jupyer Notebook as computing platform to arrange our fact and dimensional tables according to our data model.
`Python` also will be used to modify the ETL blocks/instances in the orchestration stage.

Once the dataframes are deployed into our data warehouse, we are going to code in `SQL` to fetch the tables we requiere. Likewise, we will elaborate the queries to select the information we want to display in the final dashboards. 

We will also employ some `bash` code mostly to install some depencencies in our Virtual Machine and to run some commands from within.

### Services

`Google Cloud Platform` will be the main source of cloud computing services for this project. These being:

+ Storage: `Cloud Storage`
+ Virtual Machine : `Computer Engine`
+ Analytics : `BigQuery`
+ Dashboarding :  `Looker Studio`  

For orchestrating the ETL process, we will use `Mage` as an open-source, hybrid framework for transforming and integrating our dataframes.

[Back to Table of Contents](#table-of-contents)

## Data Transformation and Modeling

Once we’ve downloaded the csv file as mentioned above, we will need to convert our flat file into some fact and dimensional tables. The former ones will contain items with high cardinality or transaction values, whereas the latter ones will be assigned to merely descriptive values. More info on some concepts of dimensional data modeling can be found [here.](https://github.com/GBlanch/Data-Engineering/blob/main/1.Uber%20Data%20Pipeline/Uber%20DE%20Transform%20and%20Model%20(LM).ipynb)


NB: Tables such as `passenger count ` or  `trip distance` would suit better to be created as fact tables - their values they keep varying- , but for the purpose on focusing on the ETL pipeline process we will create them as dimensional ones. 

The final diagram of the data model then results:

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/fa3120af-3d6e-4b44-ac04-8f130a9cd8e8"  width="70%" height="60%">


So now we’re ready to perform all the `cleaning and transformation` work before we deploy this code into Mage. The script with its explanation to the development of this code can be read [`here.`](https://github.com/GBlanch/Data-Engineering/blob/main/1.Uber%20Data%20Pipeline/Uber%20DE%20Transform%20and%20Model%20(LM).ipynb)

[Back to Table of Contents](#table-of-contents)

## Data Storage and VM configuration

Before we store our data flat file into our Cloud Storage, we have to create our Project and Bucket in GCS.

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/a3802e5c-29ab-49e6-a1b9-76b2032c100d=150x50"  width="70%" height="60%">

<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/1967e5d9-6c86-4c0b-9c61-2908d776c0e5"  width="70%" height="60%">


Once the object is uploaded, in the Permissions tab, we edit access control from `Uniform` to `Fine-grained`. 

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/fb50cce6-381d-4735-a074-b424e8a8582b"  width="40%" height="40%">

Then we go to the object and we add an entry within the `Edit Access` option so that we generate a pubic URL for this object. The next step is to set up the Computer Engine. To do that, we have to create an instance from within it. We need to select the nearest region again, the series and type of our VM as well:

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/f1ba30ed-382b-4961-a591-aea442f408e4"  width="40%" height="40%">

On the firewall section, we must allow HTTP and HTTPS traffic so that we can access it once we deploy our code.
Once the instance is created, to connect to it we just need to directly run the SSH-in-browser without the requisite of downloading any SSH keys.

Once we’re connected to the virtual machine, we run some bash commands to set up the proper environment within it. These are:

    # Update os version and install latest files
    
    	sudo apt-get install update
    
    
    # Install required Python dependencies
    
    	sudo apt-get install python3-distutils
    
    	sudo apt-get install python3-apt
    
    	sudo apt-get install wget
    
    	wget https://bootstrap.pypa.io/get-pip.py
    
    	sudo python3 get-pip.py
    
    # Install Pandas
    
    	sudo pip3 install pandas

Then we set up Mage in our virtual machine and start our project:

    #  Mage installation
    
    	sudo pip3 install mage-ai
    
    # Mage start
    	
    	mage start ude_project	

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/7473b1e4-c5de-4c3f-bfa4-94bf5e69e042" >

We can see at the last line that it is running on the indicated local host 6789. To allow our instance to accept requests from this mentioned port, we need to create a new Firewall Rule :

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/7e53f9ba-96ad-41db-afc4-ffbc468c5348"  width="70%" height="60%">

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/e1cb880c-1fd8-49a4-a609-3f566af24904"  width="70%" height="60%">

And so we have established the connection within mage from our VM.

[Back to Table of Contents](#table-of-contents)

## ETL Orchestration

<p align="center">
<img src="https://github.com/GBlanch/Data-Engineering/assets/136500426/ed3ee179-7db3-408b-bb99-ac6ed97040e1" >


+ Extract: [`uber_de_extract.py`](https://github.com/GBlanch/GCP-Uber-Data-Engineering-project/blob/main/1.Uber%20Data%20Pipeline/orchestration/batch%20pipeline/uber_de_extract.py)
  
  We pass the URL stored in our bucket so that it directly extracts data from there.  That will allow the pandas dataframe to be pulled from the bucket into this mage instance.

+ Transform: [`uber_de_transform.py`](https://github.com/GBlanch/GCP-Uber-Data-Engineering-project/blob/main/1.Uber%20Data%20Pipeline/orchestration/batch%20pipeline/uber_de_transform.py)
  
  We first import pandas in this instance as well. Next, we insert the [`cleaning and transformation code`](https://github.com/GBlanch/Data-Engineering/blob/main/1.Uber%20Data%20Pipeline/Uber%20DE%20Transform%20and%20Model%20(LM).ipynb) we developed in our LM previously using Jupyer Notebooks.

+ Load:[`uber_de_load.py`](https://github.com/GBlanch/GCP-Uber-Data-Engineering-project/blob/main/1.Uber%20Data%20Pipeline/orchestration/batch%20pipeline/uber_de_load.py)
  
  First off, we need to install the Google Cloud Service packages into our VM:

      # Install Google Cloud Library
    
    	sudo pip3 install google-cloud
    
    	sudo pip3 install google-cloud-bigquery
 
    Then we will need to create an access key so that we can pass the credentials into the default yaml file in the Load block. This key can be 
    downloaded to our LM in json format from the API & Services section in our Google console.
  
  Once the credentials are passed into our Load block in Mage, we create a dataset location within BigQuery. Besides allowing BigQuery to locate the dataframes to be received, this will also allow Mage to acknowledge the destination  of these.

Once the pipeline is successfully executed, we can check the location of these dataframes into the data warehouse BigQuery:

![image](https://github.com/GBlanch/Data-Engineering/assets/136500426/a78b369c-bb25-4c90-973f-830dbc05bc0e)

[Back to Table of Contents](#table-of-contents)

## Data Analysis

Despite the Data Eng. work has been mostly finalized, we proceed to perform some Data Analysis. 
For doing this, we will only pull the columns we want to include in our dashboards by creating specific queries. 

Hence, our new table or 'analytical layer' from which we will build our dashboard is the following:

```SQL 
CREATE OR REPLACE TABLE `uber-data-eng-19sep2023.uber_de_dataset.analytics_table` AS (
  SELECT 
    f.trip_id,
    f.VendorID,
    dt.tpep_pickup_datetime,
    dt.tpep_dropoff_datetime,
    pc.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    p.pickup_latitude,
    p.pickup_longitude,
    d.dropoff_latitude,
    d.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.tip_amount,
    f.tolls_amount,
    f.total_amount
  FROM `uber-data-eng-19sep2023.uber_de_dataset.fact_table` f
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.datetime_dim` dt  
    ON f.datetime_id = dt.datetime_id
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.passenger_count_dim` pc  
    ON pc.passenger_count_id = f.passenger_count_id  
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.trip_distance_dim` t  
    ON t.trip_distance_id = f.trip_distance_id  
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.rate_code_dim` r 
    ON r.rate_code_id = f.rate_code_id  
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.pickup_location_dim` p 
    ON p.pickup_location_id = f.pickup_location_id
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.dropoff_location_dim` d 
    ON d.dropoff_location_id = f.dropoff_location_id
  JOIN `uber-data-eng-19sep2023.uber_de_dataset.payment_type_dim` pay 
    ON pay.payment_type_id = f.payment_type_id
    )
  ;
```

We notice that we mainly have joined innerly the fact table to the rest of the dimensional tables as the [`data model`](https://github.com/GBlanch/Data-Engineering/assets/136500426/fa3120af-3d6e-4b44-ac04-8f130a9cd8e8) states.

Again, the purpose of this project wasn't to construct many series of queries but to showcase the main stages of a data pipeline using GCP. 
You can see more query developement in other repos like in any of the [`SQL-weekly-challenges`](https://github.com/GBlanch/SQL-weekly-challenges/blob/main/4.Data%20bank/query_develop/dev_query_4_31AUG23.sql) I elaborated.

[Back to Table of Contents](#table-of-contents)

## Dashboarding

After performing some quick work with Looker Studio, these are the dashboards we came up with:

![image](https://github.com/GBlanch/Data-Engineering/assets/136500426/c9b58269-741e-4a14-b202-d61a76bdf94e)

![image](https://github.com/GBlanch/Data-Engineering/assets/136500426/63b2f374-6f77-4eb5-b0cd-d618cfa15151)

![image](https://github.com/GBlanch/Data-Engineering/assets/136500426/ec42e1e0-2f3e-48db-a187-f24675e9f979)




[Back to Table of Contents](#table-of-contents)



