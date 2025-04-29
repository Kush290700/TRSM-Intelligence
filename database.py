# File: database.py

import os
import datetime
import logging
from functools import lru_cache
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# ENGINE CREATION (uses pymssql)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def get_engine():
    """
    Create a SQLAlchemy engine using the pymssql driver.
    Expects DB_SERVER, DB_NAME, DB_USER, DB_PASS in the environment.
    """
    server   = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    user     = os.getenv("DB_USER")
    pwd      = os.getenv("DB_PASS")
    if not all([server, database, user, pwd]):
        raise RuntimeError("Database credentials not set in environment variables")
    # pymssql default port is 1433
    conn_str = f"mssql+pymssql://{user}:{pwd}@{server}:1433/{database}"
    return create_engine(
        conn_str,
        fast_executemany=True,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20
    )

# ──────────────────────────────────────────────────────────────────────────────
# DATA FETCHING
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=32)
def fetch_raw_tables(start_date: str = "2020-01-01", end_date: str = None) -> dict:
    """
    Pulls all upstream tables into pandas DataFrames.
    Returns a dict of DataFrames keyed by table name.
    """
    if end_date is None:
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")

    engine = get_engine()
    params = {"start": start_date, "end": end_date}
    raw = {}

    queries = {
        "orders": text("""
            SELECT OrderId, CustomerId, SalesRepId,
                   CreatedAt AS CreatedAt_order, DateOrdered,
                   DateExpected, DateShipped AS ShipDate,
                   ShippingMethodRequested
              FROM dbo.Orders
             WHERE OrderStatus = 'packed'
               AND CreatedAt BETWEEN :start AND :end
        """),
        "order_lines": text("""
            SELECT OrderLineId, OrderId, ProductId, ShipperId,
                   QuantityShipped, Price AS SalePrice,
                   CostPrice AS UnitCost, DateShipped
              FROM dbo.OrderLines
             WHERE CreatedAt BETWEEN :start AND :end
        """),
        "customers": text("""
            SELECT CustomerId, Name AS CustomerName, RegionId, IsRetail
              FROM dbo.Customers
        """),
        "products": text("""
            SELECT ProductId, SKU, Description AS ProductName,
                   UnitOfBillingId, SupplierId, ListPrice AS ProductListPrice,
                   CostPrice
              FROM dbo.Products
        """),
        "regions": text("""
            SELECT RegionId, Name AS RegionName
              FROM dbo.Regions
        """),
        "shippers": text("""
            SELECT ShipperId, Name AS Carrier
              FROM dbo.Shippers
        """),
        "shipping_methods": text("""
            SELECT ShippingMethodId AS SMId, Name AS ShippingMethodName
              FROM dbo.ShippingMethods
        """),
        "suppliers": text("""
            SELECT SupplierId, Name AS SupplierName
              FROM dbo.Suppliers
        """),
        "packs": text("""
            WITH ol AS (
                SELECT OrderLineId
                  FROM dbo.OrderLines
                 WHERE CreatedAt BETWEEN :start AND :end
            )
            SELECT p.PickedForOrderLine, p.WeightLb, p.ItemCount,
                   p.ShippedAt AS DeliveryDate
              FROM dbo.Packs p
              JOIN ol ON p.PickedForOrderLine = ol.OrderLineId
        """)
    }

    for name, qry in queries.items():
        try:
            df = pd.read_sql(qry, engine, params=params)
            raw[name] = df
            logger.debug(f"Fetched '{name}' ({len(df):,} rows)")
        except SQLAlchemyError as e:
            logger.error(f"Error fetching '{name}': {e}")
            raw[name] = pd.DataFrame()

    return raw
