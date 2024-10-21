## Summary
For this project I created an ML pipeline for forecasting the the closing prices of stocks. This forecasting pipeline is scheduled to run daily using cron. 

My main focus was on actually building the pipeline rather than the accuracy of the forecast. To create the ML pipeline I used 3 tools: postgres, airflow, and docker.

**Postgres**
Rather than using a csv to store the data from the api requests, I chose to using a postgres database. Given the small size of the data, postgres is a bit overkill. However, I thought it would be useful to know how to set up this database  and apply other 

Additionally, to assist me in the visualization of the database I used pgadmin4. 

**Airflow**
In short, airflow is a platform used to automate workflows. This helped me simplify the process of automating my workflows. 

Similarly the postgres, using airflow is a bit overkill because my workflows were not very complicated. But I wanted to get experience using airflow.

**Docker**


**Model**
For the model, I chose to use prophet. There was not much of a debate for which model should be used because the goal of this project was to familiarise myself with this data engineering tools rather than creating the most accurate model.

**Next Steps**
* Incorporate pyspark into my data cleaning step.
* Improve the accruracy of the model
* 

