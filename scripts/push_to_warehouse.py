def push_to_warehouse(**kwargs):
    
    df = kwargs['ti'].xcom_pull(task_ids='clean_data', key='df')

    kwargs['ti'].xcom_push(key='df', value=df_no_nas)