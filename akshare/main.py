import requests
import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, INT
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr

engine = create_engine(
    "mysql+pymysql://root:Isysc0re@192.168.10.10:23306/stock_board_concept?charset=utf8&connect_timeout=5")

all_con = ak.stock_board_concept_name_ths()
for concept in all_con.name:
    df = ak.stock_board_concept_cons_ths(concept)
    #     if (concept not in engine.table_names()):
    try:
        print(concept)
        df.to_sql(concept.strip(), engine, if_exists="fail", index=False,
                  dtype={"序号": INT, "代码": INT, "名称": VARCHAR(128),
                         "现价": VARCHAR(128), "涨跌幅": VARCHAR(128), "涨跌": VARCHAR(128),
                         "涨速": VARCHAR(128), "换手": VARCHAR(128), "量比": VARCHAR(128),
                         "振幅": VARCHAR(128), "成交额": VARCHAR(128), "流通股": VARCHAR(128),
                         "流通市值": VARCHAR(128), "市盈率": VARCHAR(128)})
    except ValueError:
        continue
