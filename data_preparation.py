# File: data_preparation.py
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def prepare_full_data(raw: dict) -> pd.DataFrame:
    """
    Merge raw tables and compute Revenue, Cost, Profit, Date,
    plus delivery info (TransitDays, DeliveryStatus).
    """
    try:
        # 0. Cast all raw table join keys to string
        # Orders table
        raw['orders']['CustomerId'] = raw['orders']['CustomerId'].astype(str)
        raw['orders']['OrderId']    = raw['orders']['OrderId'].astype(str)
        raw['orders']['SalesRepId'] = raw['orders']['SalesRepId'].astype(str)
        raw['orders']['ShippingMethodRequested'] = raw['orders']['ShippingMethodRequested'].astype(str)

        # Order lines table
        raw['order_lines']['OrderId']      = raw['order_lines']['OrderId'].astype(str)
        raw['order_lines']['OrderLineId']  = raw['order_lines']['OrderLineId'].astype(str)
        raw['order_lines']['ProductId']    = raw['order_lines']['ProductId'].astype(str)
        raw['order_lines']['ShipperId']    = raw['order_lines']['ShipperId'].astype(str)

        # Lookup tables
        raw['customers']['CustomerId']     = raw['customers']['CustomerId'].astype(str)
        raw['customers']['RegionId']       = raw['customers']['RegionId'].astype(str)

        raw['products']['ProductId']       = raw['products']['ProductId'].astype(str)
        raw['products']['SupplierId']      = raw['products']['SupplierId'].astype(str)

        raw['regions']['RegionId']         = raw['regions']['RegionId'].astype(str)
        raw['shippers']['ShipperId']       = raw['shippers']['ShipperId'].astype(str)
        raw['suppliers']['SupplierId']     = raw['suppliers']['SupplierId'].astype(str)

        raw['shipping_methods'] = raw['shipping_methods'].rename(
            columns={'SMId':'ShippingMethodRequested'}
        )
        raw['shipping_methods']['ShippingMethodRequested'] = \
            raw['shipping_methods']['ShippingMethodRequested'].astype(str)

        # 1. Merge orders and order_lines
        df = raw['orders'].merge(
            raw['order_lines'], on='OrderId', how='inner'
        )

        # 2. Static lookups
        joins = {
            'customers': ('CustomerId', raw['customers']),
            'products':  ('ProductId', raw['products']),
            'regions':   ('RegionId', raw['regions']),
            'shippers':  ('ShipperId', raw['shippers']),
            'suppliers': ('SupplierId', raw['suppliers']),
            'smethods':  ('ShippingMethodRequested', raw['shipping_methods'])
        }
        for _, (col, tbl) in joins.items():
            df = df.merge(tbl, on=col, how='left')

        # 3. Packs aggregation
        packs = raw['packs']
        if not packs.empty:
            packs['PickedForOrderLine'] = packs['PickedForOrderLine'].astype(str)
            packs['OrderLineId'] = packs['PickedForOrderLine']
            psum = (
                packs.groupby('OrderLineId', as_index=False)
                .agg(
                    WeightLb=('WeightLb','sum'),
                    ItemCount=('ItemCount','sum'),
                    DeliveryDate=('DeliveryDate','max')
                )
            )
            df = df.merge(psum, on='OrderLineId', how='left')
            df[['WeightLb','ItemCount']] = df[['WeightLb','ItemCount']].fillna(0)

        # 4. Numeric safety
        numcols = ['QuantityShipped','SalePrice','UnitCost','WeightLb','ItemCount']
        df[numcols] = df[numcols].apply(
            pd.to_numeric, errors='coerce'
        ).fillna(0.0)

        # 5. Compute shipped weight
        per_item = df['WeightLb'] / df['ItemCount'].replace(0, np.nan)
        df['ShippedWeightLb'] = np.where(
            (df['UnitOfBillingId'] == 3) & (df['WeightLb'] > 0),
            df['WeightLb'],
            df['ItemCount'] * per_item.fillna(0)
        )

        # 6. Revenue, Cost, Profit
        is_weight = (df['UnitOfBillingId'] == 3) & (df['WeightLb'] > 0)
        df['Revenue'] = np.where(
            is_weight,
            df['WeightLb'] * df['SalePrice'],
            df['ItemCount'] * df['SalePrice']
        )
        df['Cost'] = np.where(
            is_weight,
            df['WeightLb'] * df['UnitCost'],
            df['ItemCount'] * df['UnitCost']
        )
        df['Profit'] = df['Revenue'] - df['Cost']

        # 7. Date and delivery metrics
        df['Date'] = pd.to_datetime(
            df['CreatedAt_order'], errors='coerce'
        ).dt.normalize()
        df['ShipDate']     = pd.to_datetime(df['ShipDate'], errors='coerce')
        df['DeliveryDate'] = pd.to_datetime(df['DeliveryDate'], errors='coerce')
        df['TransitDays']  = (
            df['DeliveryDate'] - df['ShipDate']
        ).dt.days.clip(lower=0)
        df['DeliveryStatus'] = np.where(
            df['DeliveryDate'] <= pd.to_datetime(
                df['DateExpected'], errors='coerce'
            ),
            'On Time', 'Late'
        )

        logger.info(f"Prepared data: {len(df)} rows")
        return df

    except Exception:
        logger.exception("Data prep failed")
        raise