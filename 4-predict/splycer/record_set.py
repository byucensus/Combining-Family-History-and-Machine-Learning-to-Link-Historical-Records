#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: profile=True
"""
Created on Mon Jul 22 14:02:20 2019

@author: Nick Grasley (ngrasley@stanford.edu)
"""
import numpy as np
import pandas as pd
import turbodbc
from turbodbc import make_options
from splycer.base import RecordBase


class RecordDict(dict,
                 RecordBase):  # FIXME update the output format of data from this class. It should be a Pandas DataFrame.
    """Records are organized in a dictionary with the key as a unique identifier
       and the value as a numpy structured array of record info. Since dictionary
       lookup scales at a constant rate with the number of records, this object
       is most efficient when you merely have to grab record information. This
       assumes the record arrays are numpy structured arrays.
    """

    def __init__(self, record_id, uids, features):
        self.record_id = record_id
        self.var_list = None
        super().__init__(zip(uids, features))

    def set_var_list(self, var_list):
        self.var_list = var_list

    def get_record(self, uid):
        if self.var_list is None:
            return pd.DataFrame.from_records(self.get(uid))
        return pd.DataFrame.from_records(self.get(uid)[self.var_list])

    def get_records(self, uids):
        rec_array = np.concatenate(tuple(self.get(i) for i in uids))
        if self.var_list is None:
            return pd.DataFrame.from_records(rec_array)
        return pd.DataFrame.from_records(rec_array)[self.var_list]


class RecordDB(
    RecordBase):
    """Records are stored in a sql database. If you need to do any blocking,
       sql handles a lot of the hard work of building data structures for efficient
       merges. You have to pay the upfront cost of setting up the database though.
    """

    def __init__(self, table_name, idx_name, dsn, extra_joins='', var_list=None):
        '''
        table_name(string): name of SQL table that stores records
        idx_name(string): column name that stores unique identifier for a record (row from table specified)
        dsn(string): dsn ODBC string to connect to SQL through turbodbc driver
        extra_joins(string): extra SQL code the end of get_record{s}(). Can be used to create extra joins for getting
        data from secondary tables or for more WHERE filters
        '''
        self.var_list = var_list
        self.table_name = table_name
        self.idx_name = '['+idx_name+']'
        options = make_options()
        self.conn = turbodbc.connect(dsn=dsn, turbodbc_options=options)
        self.cursor = self.conn.cursor()
        self.extra_joins = extra_joins
        """
        self.cursor.execute(f"select column_name from information_schema.columns where table_name = '{self.table_name}'")
        cols = self.cursor.fetchall()
        self.cols = np.ndarray(len(cols), dtype="U50")
        for i in range(len(cols)):
            self.cols[i] = cols[i][0]
        """

    def set_var_list(self, var_list):
        self.var_list = var_list

    def set_joins(self, join_str):
        self.extra_joins = join_str

    def __getitem__(self, uid):
        if isinstance(uid, list):
            return self.get_records(uid)
        return self.get_record(uid)

    def get_record(self, uid):
        if self.var_list is None:
            data = pd.read_sql(f"select * from {self.table_name} where {self.idx_name} = {uid} {self.extra_joins}",
                               self.conn)
        else:
            data = pd.read_sql(
                f"select {self.var_list} from {self.table_name} where {self.idx_name} = {uid} {self.extra_joins}",
                self.conn)
        return data

    def get_records(self, uids, chunksize = None,):
        '''
        uids(list-like): indices of records you want to extract from SQL table
        chunksize(int): chunksize parameter to be passed into pd.read_sql() WARNING: Does not really work with our setup

        Returns (pandas DataFrame): returns rows from specified SQL table associated with uids that are passed in
        '''
        print("Inserting values")
        indices = np.expand_dims(np.unique(np.array(uids)), axis=0).T.tolist()
        # FIXME: Not sure if this is as necessary as I thought it was
        # create new table that doesn't exist. This allows for multiple users to run get_records
        # at once without writing/reading from same temporary table
        table_exists, i = True, 1
        '''
        while table_exists:
            i += 1
            table_exists = \
                self.cursor.execute(
                    f"if object_id('dbo.temp_idx', 'U') is not null select 1 else select 0").fetchone()[
                    0]
        '''
        self.cursor.execute('drop table if exists temp_idx')
        self.conn.commit()
        self.cursor.execute(f'create table temp_idx ([index_] int)')  # create temporary table
        self.conn.commit()
        self.cursor.executemany(f'INSERT INTO temp_idx VALUES (?)',
                                indices)  # insert target people indices into new table
        self.conn.commit()
        print("Done inserting values")

        # merge temporary table indices onto record_set table
        if self.var_list is None:
            print("here in record_set.py")
            self.cursor.execute(f"select * from temp_idx as t1 inner join {self.table_name} as t2 on (t1.[index_]=t2.{self.idx_name}) {self.extra_joins}")
            table = self.cursor.fetchallarrow()
            data = table.to_pandas()
            print("done")
        else:
            data = pd.read_sql(
                f"select {self.var_list} from temp_idx as t1 inner join {self.table_name} as t2 on (t1.[index_]=t2.{self.idx_name}) {self.extra_joins}",
                self.conn,chunksize=chunksize)
            print(data)

        self.conn.commit()
        self.cursor.execute(f'drop table temp_idx')  # delete temp table after merge complete
        #uids = pd.Series(uids)
        #uids = uids[uids.isin(data['index']).tolist()].tolist() # return in order of index list that is passed in
        #return data.set_index('index').loc[uids].reset_index().rename(columns={'index': self.idx_name})
        self.conn.commit()
        return data


class RecordDataFrame(RecordBase):
    """Records are stored in a Pandas DataFrame. This is best for small datasets
       with a limited number of string features, or if you need to block and don't
       want to set up a sql server. However, it has slow lookup, slow merges, and
       is a memory hog, so don't use this for large datasets.
    """

    def __init__(self, record_id, records, uid_col=None):
        self.record_id = record_id
        self.var_list = None
        if type(records) == pd.core.frame.DataFrame:
            self.df = records
        else:
            self.df = pd.DataFrame(records, index=uid_col)

    def set_var_list(self, var_list):
        self.var_list = var_list

    def __getitem__(self, uid):
        return self.df.loc[uid, :]

    def get_record(self, uid):
        if self.var_list is None:
            return self.df.loc[[uid], :].reset_index(drop=True)
        return self.df.loc[[uid], self.var_list].reset_index(drop=True)

    def get_records(self, uids):
        if self.var_list is None:
            return self.df.loc[uids, :].reset_index(drop=True)
        return self.df.loc[uids, self.var_list].reset_index(drop=True)
