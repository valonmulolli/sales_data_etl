"""Data access endpoints for the ETL pipeline API."""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, text

from config import DB_CONFIG
from models import SalesRecord

router = APIRouter()


@router.get("/data/sales")
async def get_sales_data(
    start_date: Optional[date] = Query(None, description="Start date for filtering"),
    end_date: Optional[date] = Query(None, description="End date for filtering"),
    product_id: Optional[str] = Query(None, description="Filter by product ID"),
    limit: int = Query(100, description="Number of records to return"),
    offset: int = Query(0, description="Number of records to skip"),
) -> Dict[str, Any]:
    """Get sales data with optional filtering."""
    try:
        engine = create_engine(DB_CONFIG["connection_string"])

        # Build query
        query = "SELECT * FROM sales_records WHERE 1=1"
        params = {}

        if start_date:
            query += " AND date >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params["end_date"] = end_date

        if product_id:
            query += " AND product_id = :product_id"
            params["product_id"] = product_id

        query += " ORDER BY date DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            records = [dict(row) for row in result]

        return {
            "data": records,
            "total": len(records),
            "limit": limit,
            "offset": offset,
            "filters": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "product_id": product_id,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch sales data: {str(e)}"
        )


@router.get("/data/sales/summary")
async def get_sales_summary(
    start_date: Optional[date] = Query(None, description="Start date for summary"),
    end_date: Optional[date] = Query(None, description="End date for summary"),
) -> Dict[str, Any]:
    """Get sales summary statistics."""
    try:
        engine = create_engine(DB_CONFIG["connection_string"])

        query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(quantity) as total_quantity,
            SUM(total_sales) as total_revenue,
            AVG(unit_price) as avg_unit_price,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT date) as unique_dates
        FROM sales_records
        WHERE 1=1
        """
        params = {}

        if start_date:
            query += " AND date >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params["end_date"] = end_date

        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            summary = dict(result.fetchone())

        return {
            "summary": summary,
            "period": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch sales summary: {str(e)}"
        )


@router.get("/data/sales/by-product")
async def get_sales_by_product(
    start_date: Optional[date] = Query(None, description="Start date for filtering"),
    end_date: Optional[date] = Query(None, description="End date for filtering"),
) -> Dict[str, Any]:
    """Get sales data aggregated by product."""
    try:
        engine = create_engine(DB_CONFIG["connection_string"])

        query = """
        SELECT 
            product_id,
            COUNT(*) as total_records,
            SUM(quantity) as total_quantity,
            SUM(total_sales) as total_revenue,
            AVG(unit_price) as avg_unit_price,
            AVG(discount) as avg_discount
        FROM sales_records
        WHERE 1=1
        """
        params = {}

        if start_date:
            query += " AND date >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params["end_date"] = end_date

        query += " GROUP BY product_id ORDER BY total_revenue DESC"

        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            products = [dict(row) for row in result]

        return {
            "products": products,
            "total_products": len(products),
            "period": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch product data: {str(e)}"
        )


@router.get("/data/sales/by-date")
async def get_sales_by_date(
    start_date: Optional[date] = Query(None, description="Start date for filtering"),
    end_date: Optional[date] = Query(None, description="End date for filtering"),
) -> Dict[str, Any]:
    """Get sales data aggregated by date."""
    try:
        engine = create_engine(DB_CONFIG["connection_string"])

        query = """
        SELECT 
            date,
            COUNT(*) as total_records,
            SUM(quantity) as total_quantity,
            SUM(total_sales) as total_revenue,
            AVG(unit_price) as avg_unit_price,
            COUNT(DISTINCT product_id) as unique_products
        FROM sales_records
        WHERE 1=1
        """
        params = {}

        if start_date:
            query += " AND date >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params["end_date"] = end_date

        query += " GROUP BY date ORDER BY date DESC"

        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            dates = [dict(row) for row in result]

        return {
            "daily_sales": dates,
            "total_days": len(dates),
            "period": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch daily data: {str(e)}"
        )


@router.post("/data/upload")
async def upload_sales_data(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Upload new sales data."""
    try:
        # Validate data
        if not data:
            raise HTTPException(status_code=400, detail="No data provided")

        # Convert to DataFrame for validation
        df = pd.DataFrame(data)
        required_columns = [
            "date",
            "product_id",
            "quantity",
            "unit_price",
            "discount",
            "total_sales",
        ]

        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise HTTPException(
                status_code=400, detail=f"Missing required columns: {missing_columns}"
            )

        # Load data using existing loader
        from load import SalesDataLoader

        loader = SalesDataLoader()
        loader.load_to_database(df)

        return {
            "message": "Data uploaded successfully",
            "records_processed": len(data),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload data: {str(e)}")
