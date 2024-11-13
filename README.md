## Summary
For this project I created an ML pipeline to forecast the closing prices of stocks. This forecasting pipeline is scheduled to run daily using cron. Specifically, at 12am the latest stock data is gathered, cleaned, and pushed to the database. At 2 am a model training workflow is run which forecasts the stock closing price over the next 5 days.

<p align="center">
    <img src="https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54" alt="Python">
    <img src="https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas">
    <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="PySpark">
    <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white" alt="Apache Airflow">
    <img src="https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
    <img src="https://img.shields.io/badge/pgAdmin-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="pgAdmin">
    <img src="https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
    <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit">
</p>


My main focus was on building the pipeline rather than the accuracy of the forecast.

![alt text](./source_images/finance_pipeline.png)

**Postgres**

Rather than using a csv to store the data from the api requests, I used a postgres database. Given the small size of the data, postgres is a bit overkill. However, I thought it would be useful to know how to set up a database from scratch.  

To assist me in the visualization of the database I used pgadmin4. 

**Airflow**

In short, airflow is a platform used to automate workflows.

Similarly to postgres, using airflow is a bit overkill because my workflows were not very complicated. I used airflow to create 3 workflows:
* A workflow to delete the current database (this was mostly used in testing).
* A workflow to get the api data and push it to the postgres database.
* A workflow to create forecasts.

I have attached images of the push_to_postgres and forecast workflow below.

![alt text](./source_images/push_to_postgres_workflow.png)
![alt text](./source_images/forecast_workflow.png)

**Docker**

To simplify the process of setting up a database and to streamline the pipeline creation process I used docker. 

**Streamlit**

Finally, to visualize the results, I created a container to run a streamlit script to display the closing price forecasts.

**PySpark**

For this project I wanted to try to use pyspark in the api data cleaning step. I was successful in installing pyspark however I've realised pyspark does not perform well for smaller datasets. Consequently, I still use the inital scripts which resorts to using pandas.

**Model**

Since the goal of this project was to familiarise myself with this data engineering tools rather than creating the most accurate model, I chose to use a Prophet model.

As hypothesized, the model's forecasts were unimpressive.
![Alt text](./source_images/streamlit_example.png)

**Next Steps**
* Improve the accruracy of the model.
* Incorporate monthly forecasts.
* Add pre-commits.
* Investigate weird forecasts for some firms.
