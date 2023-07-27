from dagster import asset, AssetIn, SourceAsset, AssetKey
import os 
import duckdb
import pandas as pd

con = duckdb.connect()

source_asset = SourceAsset(key=AssetKey("some_asset"))

@asset(compute_kind="duckdb", io_manager_key="io_manager")
def raw_customers() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of customer data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_customers_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_location() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of location data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_geolocation_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_order_items_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of order items data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_order_items_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_order_payments_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of order payment data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_order_payments_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_order_reviews_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of order reviews data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_order_reviews_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_orders_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of orders data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_orders_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df


@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_products_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of products data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_products_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_sellers_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of sellers data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/olist_sellers_dataset.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

@asset(compute_kind="duckdb", io_manager_key=	"io_manager")
def raw_product_translations_dataset() -> pd.DataFrame:
    """
    Retrieves a raw csv file from which we can generate a pandas dataframe of product translations data. 
    """
    csv = duckdb.read_csv("./location_data_generators/assets/raw_data/seed/product_category_name_translation.csv", header=True, sep=",")
    resulting_df = csv.fetchdf() # fetchdf() is a method that returns a pandas dataframe
    return resulting_df

