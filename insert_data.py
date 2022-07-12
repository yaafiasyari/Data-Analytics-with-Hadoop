#!python3

import os
import json

import pandas
from sqlalchemy import create_engine

if __name__ == "__main__":
    path = os.getcwd() + "\\" + "dataset" + "\\"

    for dic in [("TR_OrderDetails.csv","fact_orderdetails"),
                ("TR_Products.csv","dim_products"),
                ("TR_PropertyInfo.csv","dim_location"),
                ("TR_UserInfo.csv","dim_users")]:
        
        df = pandas.read_csv(path + dic[0])
        engine = create_engine('postgresql://postgres:buyung12345@localhost:5432/project4')
        df.to_sql(dic[1], engine, if_exists='replace', index=False)