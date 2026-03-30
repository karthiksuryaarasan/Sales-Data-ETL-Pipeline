from sqlalchemy import create_engine
import pandas as pd

def extract():
    df = pd.read_csv("data/sales.csv")
    return df

def transform(df):
    df['amount'] = df['amount'].astype(float)
    df['order_date'] = pd.to_datetime(df['order_date'])
    df = df.drop_duplicates()
    return df

def load(df):
    db_url = "postgresql://postgres:postgres@localhost:5432/sales_db"
    
    engine = create_engine(db_url)

    df.to_sql(
        "sales",
        engine,
        if_exists="replace",
        index=False
    )

def run():
    df = extract()
    df = transform(df)
    load(df)
    print("ETL Completed Successfully")

if __name__ == "__main__":
    run()