import pandas as pd
import requests
from sqlalchemy import *
from config import configuration
from input import input


def ExceptionHandle(function):
    def ErrHndl(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as err:
            print("\nException occurred during run of {} : {} in {}.....".format(kwargs['dataset_name'], err, function))

    return ErrHndl


def get_data_url(*args, **kwargs):
    """ Simple response function to GET the data set """

    return requests.get(**kwargs)


@ExceptionHandle
def insert_db(df, db_deets=configuration['db'], **kwargs):
    """ Initialises the SQLlite DB for table load using the SQL Alchemy Engine, since JSON or onject handling is a bit tricky in SqlLite, using normalized
        inserts for now, will switch to postgres if there is no alternative"""

    with create_engine(db_deets["database"]).connect() as conn:
        conn.execute("DELETE FROM {}".format(kwargs['target_table']["name"]))
        df.to_sql(con=conn, **kwargs['target_table'])


@ExceptionHandle
def get_data_df(*args, **kwargs):
    """ Returns Pandas Dataframe based on the input file conditions, can handle JSON, variable length and fixed length return types
        Normalisation and drop columns must be supplied in the input until I figure out a great method to handle JSONB or Arrays"""

    dataset = get_data_url(**kwargs['request'])

    if (dataset.text)[0] in '{[':
        if kwargs['pd_dataframe']['norm']:
            df = pd.json_normalize(data=dataset.json(), **kwargs['pd_dataframe']['norm']).drop(
                kwargs['pd_dataframe']['drop_cols'], axis=1)
        else:
            df = pd.json_normalize(data=dataset.json())
    elif kwargs['pd_dataframe']['sep']:
        df = pd.read_csv(StringIO(dataset.text), **kwargs['pd_dataframe']).drop(kwargs['pd_dataframe']['drop_cols'],
                                                                                axis=1)
    elif kwargs['pd_dataframe']['colspecs']:
        df = pd.read_fwf(StringIO(dataset.text), **kwargs['pd_dataframe']).drop(kwargs['pd_dataframe']['drop_cols'],
                                                                                axis=1)
    else:
        print('not covered')

    if kwargs['write_to_csv']:
        df.to_csv(kwargs['dataset_name'] + '.csv')

    return df


def main():
    print("\n################---------Starting ETL Process--------######################")
    try:
        for i in input['etl']:
            insert_db(get_data_df(**i), **i)

    except Exception as error:
        print("\nError when starting ETL Process - please check below, exiting..\n{}".format(error))

    print("\n################---------ETL Process Complete--------######################")


if __name__ == "__main__":
    main()
