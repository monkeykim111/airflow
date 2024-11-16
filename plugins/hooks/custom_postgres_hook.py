from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd


class CustomPostgresHook(BaseHook):
    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)  # connection 정보 반환
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn  # postgres connection session 정보 반환
    
    def bulk_load(self, table_name, file_name, delimiter: str, is_header:bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상 파일:' + file_name)
        self.log.info('테이블:' + table_name)
        self.get_conn()
        header = 0 if is_header else None # is_header = true면 0, false면 None
        if_exists = 'replace' if is_replace else 'append' # is_replace = true면 replace, false면 append
        
        try:
            file_df = pd.read_csv(file_name, header=header, delimiter=delimiter, encoding='utf-8')
        except UnicodeDecodeError:
            self.log.info("UTF-8 인코딩 실패. EUC-KR로 재시도합니다.")
            file_df = pd.read_csv(file_name, header=header, delimiter=delimiter, encoding='euc-kr')

        for col in file_df.columns:
            try:
                # string인 경우에만 처리
                file_df[col] = file_df[col].str.replace('\r\n', '')
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                # string 문자열이 아닐 경우 continue
                continue

        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                       con=engine,
                       schema='public',
                       if_exists=if_exists,
                       index=False
                       )

