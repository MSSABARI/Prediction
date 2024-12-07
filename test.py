from fastapi import APIRouter, Query, HTTPException
from datetime import datetime,timedelta
import pandas as pd
from statsmodels.tsa.stattools import adfuller
import logging ,random 
from rdb.co import collection ,prediction_collection
from statsmodels.tsa.statespace.sarimax import SARIMAX
from demo.schemas import BaseModel ,ForecastData
from typing import List 


TRouter = APIRouter()

def parse_iso_datetime(iso_str: str) -> datetime:
   
    try:
        return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    except ValueError as e:
        raise ValueError(f"Invalid date format: {iso_str}. Error: {str(e)}")


def get_data(collection, start_date: datetime, end_date: datetime, meter: int, _ID: str):
    try:
        # Log query parameters
        print(f"Query Parameters - Start Date: {start_date}, End Date: {end_date}, Meter: {meter}, Object ID: {_ID}")

        # Query the database for nested data
        data = collection.find(
            {
                "createdAt": {"$gte": start_date, "$lte": end_date},
                "meter": meter,
                f"data.{_ID}": {"$exists": True}  # Ensure the nested data object exists
            },
            {
                f"data.{_ID}.value": 1,  # Include the value
                "createdAt": 1,  # Include createdAt
                "meter": 1  # Include meter
            }
        )

        fetched_data = []
        for item in data:
            nested_data = item['data'][_ID]
            fetched_data.append({
                "createdAt": item['createdAt'],
                "meter": item['meter'],
                "value": nested_data.get('value')
            })

        return fetched_data
    except Exception as e:
        logging.error(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query error.")



@TRouter.get("/forecasting", response_model=List[ForecastData])
def forecast_data(
    type: str = Query(..., description="Forecast type, e.g., forecast-kilowatt-data, forecast-current-data, forecast-voltage-data"),
    meterID: int = Query(...),
    startDate: str = Query(...),
    endDate: str = Query(...),
    timePeriod: str = Query(..., description="Forecast period, e.g., 'day', 'week', or 'month'")
):
    try:
        # Parse dates
        start = datetime.fromisoformat(startDate)
        end = datetime.fromisoformat(endDate)
        print(f"Parsed Dates - Start: {start}, End: {end}")

        # Map type to object_id
        if type == "forecast-kilowatt-data":
            object_id = "66f0efcdf65db44ec9603972"
        elif type == "forecast-current-data":
            object_id = "66f0f06ef65db44ec960398a"
        elif type == "forecast-voltage-data":
            object_id = "66f0f039f65db44ec9603982"
        else:
            raise HTTPException(status_code=400, detail="Invalid forecast type.")

        # Adjust time period
        if timePeriod == "week":
            start -= timedelta(days=start.weekday() + 1)
            end = start + timedelta(days=6)
        elif timePeriod == "month":
            start = start.replace(day=1)
            end = (start.replace(month=start.month % 12 + 1, day=1) - timedelta(days=1))
        elif timePeriod != "day":
            raise HTTPException(status_code=400, detail="Invalid time period.")

        # Retrieve data from the collection
        data = get_data(collection, start, end, meterID, object_id)
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found for object ID {object_id}")

        # Convert data to DataFrame
        df = pd.DataFrame(data)
        print(f"Original Data: {df.tail()}")  # Log the last few rows of original data

        if 'value' not in df.columns or df['value'].isnull().all():
            raise HTTPException(status_code=500, detail="No valid 'value' data found.")
        
        # Fill missing values in 'value' and handle timestamp conversion
        df['value'] = df['value'].fillna(0)
        df['ds'] = pd.to_datetime(df['createdAt'], errors='coerce')
        
        if df['ds'].isnull().any():
            raise HTTPException(status_code=500, detail="Invalid 'createdAt' timestamps.")
        
        df['y'] = df['value']
        df.set_index('ds', inplace=True)

        # Resample data based on the selected time period
        if timePeriod == 'day':
            df_resampled = df.resample('1min').ffill()  # Resample every minute for day data
            forecast_periods, freq = 1440, '1min'
        elif timePeriod == 'week':
            df_resampled = df.resample('15min').ffill()  # Resample every 15 minutes for weekly data
            forecast_periods, freq = 672, '15min'
        elif timePeriod == 'month':
            df_resampled = df.resample('1h').ffill()  # Resample every hour for monthly data
            forecast_periods, freq = 720, '1h'
        else:
            raise HTTPException(status_code=400, detail="Invalid time period.")

        print(f"Resampled Data: {df_resampled.tail()}")  # Log the resampled data

        # Forecasting with SARIMA
        try:
            model = SARIMAX(
                df_resampled['y'],
                order=(1, 1, 1),
                seasonal_order=(1, 1, 1, 24),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            model_fit = model.fit(disp=False)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"SARIMA model error: {e}")

        # Generate forecast values
        forecast_index = pd.date_range(
            start=df_resampled.index[-1] + pd.Timedelta(freq),
            periods=forecast_periods,
            freq=freq
        )

        forecast_values = model_fit.forecast(steps=forecast_periods)
        print(f"Forecast Values: {forecast_values[:5]}")  # Log first few forecasted values

        # Prepare forecasted data with actual and forecasted values
        forecasted_data = []
        for date, forecast_value, actual_value in zip(forecast_index, forecast_values, df['value'].iloc[-len(forecast_values):]):
            yhat = forecast_value  # Forecasted value
            forecasted_data.append({
                "id": object_id,
                "meter": meterID,
                "data": {
                    "value": actual_value,  # Actual value from the database
                    "createdAt": date.isoformat(),
                    "yhat": yhat,  # Forecasted value
                    "ds": date.isoformat()
                }
            })

        # Log forecasted data before inserting
        print(f"Forecasted Data: {forecasted_data[:5]}")  # Log first few forecasted data points

        # Insert predictions if they do not already exist
        for forecast in forecasted_data:
            if not prediction_collection.find_one({"meter": meterID, "data.createdAt": forecast['data']['createdAt']}):
                prediction_collection.insert_one(forecast)

        return forecasted_data

    except Exception as e:
        logging.error(f"Error during forecasting: {e}")
        raise HTTPException(status_code=500, detail=str(e))