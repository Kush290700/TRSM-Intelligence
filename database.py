# File: database.py

import os
import datetime
import logging
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ─── Logging setup ─────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Engine factory ────────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def get_engine():
    """
    Returns a SQLAlchemy engine using the pymssql driver,
    which can be installed via pip (`pymssql`) without any OS-level ODBC deps.
    """
    server   = os.getenv("DB_SERVER", "10.4.21.5")
    database = os.getenv("DB_NAME",   "TRSM")
    user     = os.getenv("DB_USER",   "TRSMAna")
    pwd      = os.getenv("DB_PASS",   "chattypostgraduatecanary")

    # pymssql URL schema — no external ODBC driver required
    conn_str = f"mssql+pymssql://{user}:{pwd}@{server}/{database}"

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

# ─── Data fetcher ──────────────────────────────────────────────────────────────
@lru_cache(maxsize=32)
def fetch_raw_tables(start_date: str = "2020-01-01", end_date: str = None) -> dict:
    """
    Pulls all raw tables from the TRSM database between start_date and end_date.
    Caches up to 32 distinct (start,end) parameter pairs.
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
             WHERE OrderStatus='packed'
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
                   UnitOfBillingId, SupplierId,
                   ListPrice AS ProductListPrice, CostPrice
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
            raw[name] = pd.read_sql(qry, engine, params=params)
            logger.debug(f"Fetched '{name}': {len(raw[name])} rows")
        except SQLAlchemyError as e:
            logger.error(f"Error fetching '{name}': {e}")
            raw[name] = pd.DataFrame()

    return raw
