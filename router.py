from fastapi import APIRouter, Query, HTTPException
from datetime import datetime, timedelta
import pandas as pd
from statsmodels.tsa.stattools import adfuller
import logging ,random
from Database.config import kilowatt_collection , current_collection , voltage_collection ,db
from statsmodels.tsa.statespace.sarimax import SARIMAX

TestRouter = APIRouter()

def parse_iso_datetime(iso_str: str) -> datetime:
   
    try:
        return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    except ValueError as e:
        raise ValueError(f"Invalid date format: {iso_str}. Error: {str(e)}")

def get_data(collection, start_date: datetime, end_date: datetime, meter: int):
   
    try:
        data = collection.find(
            {
                "createdAt": {"$gte": start_date, "$lte": end_date},
                "meter": meter
            },
            {"_id": 0, "data.value": 1, "createdAt": 1, "meter": 1}
        )
        return list(data)
    except Exception as e:
        logging.error(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query error.")
    
@TestRouter.get("/current-data")
def current_data(
    start_time: str = Query(...),
    end_time: str = Query(...),
    meter_ID: int = Query(...),
    type: str = Query(..., description="Type of forecast data to include ('forecast-current-data' or 'forecast-voltage-data')"),
    time_period: str = Query(..., description="Time period for data ('day', 'week', 'month')")
):
    start = parse_iso_datetime(start_time)
    end = parse_iso_datetime(end_time)

    if time_period == "day":

        pass
    elif time_period == "week":

        start = start - timedelta(days=(start.weekday() + 1) % 7)
        end = start + timedelta(days=7)  
    elif time_period == "month":
       
        start = start.replace(day=1) 
       
        next_month = start.replace(day=28) + timedelta(days=4) 
        end = next_month - timedelta(days=next_month.day)  
    else:
        raise HTTPException(status_code=400, detail="Invalid time period. Use 'day', 'week', or 'month'.")

    print(f"Current Data - Start: {start}, End: {end}, Meter: {meter_ID}, Type: {type}, Time Period: {time_period}")

    data = get_data(current_collection, start, end, meter_ID)

    if not data:
        raise HTTPException(
            status_code=404, 
            detail=f"No current data found for meter {meter_ID} within the given time range."
        )


    response_data = []
    for record in data:
        created_at = record.get("createdAt")
        value = record.get("data", {}).get("value")

        response_data.append({
            "meter": record.get("meter"),
            "data": {
                "value": value,
                "createdAt": created_at
            }
        })

    if time_period == "week":
        response_data = adjust_for_time_gap(response_data, timedelta(minutes=30))
    elif time_period == "month":
        response_data = adjust_for_time_gap(response_data, timedelta(hours=2))

    return {"data": response_data}


@TestRouter.get("/voltage-data")
def voltage_data(
    start_time: str = Query(...),
    end_time: str = Query(...),
    meter_ID: int = Query(...),
    type: str = Query(..., description="Type of forecast data to include ('forecast-current-data' or 'forecast-voltage-data')"),
    time_period: str = Query(..., description="Time period for data ('day', 'week', 'month')")
):
    start = parse_iso_datetime(start_time)
    end = parse_iso_datetime(end_time)

    if time_period == "day":
        pass
    elif time_period == "week":
        start = start - timedelta(days=(start.weekday() + 1) % 7)
        end = start + timedelta(days=7)  
    elif time_period == "month":
        start = start.replace(day=1) 
        next_month = start.replace(day=28) + timedelta(days=4)
        end = next_month - timedelta(days=next_month.day) 
    else:
        raise HTTPException(status_code=400, detail="Invalid time period. Use 'day', 'week', or 'month'.")

    print(f"Voltage Data - Start: {start}, End: {end}, Meter: {meter_ID}, Type: {type}, Time Period: {time_period}")

    data = get_data(voltage_collection, start, end, meter_ID)

    if not data:
        raise HTTPException(
            status_code=404, 
            detail=f"No voltage data found for meter {meter_ID} within the given time range."
        )

    response_data = []
    for record in data:
        created_at = record.get("createdAt")
        value = record.get("data", {}).get("value")

        response_data.append({
            "meter": record.get("meter"),
            "data": {
                "value": value,
                "createdAt": created_at
            }
        })

    if time_period == "week":
        response_data = adjust_for_time_gap(response_data, timedelta(minutes=30))
    elif time_period == "month":
        response_data = adjust_for_time_gap(response_data, timedelta(hours=2))

    return {"data": response_data}
    
@TestRouter.get("/kilowatt-data")
def kilowatt_data(
    start_time: str = Query(...),
    end_time: str = Query(...),
    meter_ID: int = Query(...),
    type: str = Query(..., description="Type of forecast data to include ('forecast-kilowatt-data')"),
    time_period: str = Query(..., description="Time period for data ('day', 'week', 'month')")
):
    # Parse the start and end times
    start = parse_iso_datetime(start_time)
    end = parse_iso_datetime(end_time)

    # Adjust time ranges based on the time period
    if time_period == "day":
        pass
    elif time_period == "week":
        start = start - timedelta(days=(start.weekday() + 1) % 7)
        end = start + timedelta(days=7)  
    elif time_period == "month":
        start = start.replace(day=1)
        next_month = start.replace(day=28) + timedelta(days=4)
        end = next_month - timedelta(days=next_month.day) 
    else:
        raise HTTPException(status_code=400, detail="Invalid time period. Use 'day', 'week', or 'month'.")

    print(f"Kilowatt Data - Start: {start}, End: {end}, Meter: {meter_ID}, Type: {type}, Time Period: {time_period}")

    # Fetch data from the KILOWATT_AVG collection
    kilowatt_collection = db["KILOWATT_AVG"]
    data = get_data(kilowatt_collection, start, end, meter_ID)

    # Check if data is found
    if not data:
        raise HTTPException(
            status_code=404, 
            detail=f"No kilowatt data found for meter {meter_ID} within the given time range."
        )

    # Prepare the response
    response_data = []
    for record in data:
        created_at = record.get("createdAt")
        value = record.get("data", {}).get("value")

        response_data.append({
            "meter": record.get("meter"),
            "data": {
                "value": value,
                "createdAt": created_at
            }
        })

    # Adjust for time gaps based on the time period
    if time_period == "week":
        response_data = adjust_for_time_gap(response_data, timedelta(minutes=30))
    elif time_period == "month":
        response_data = adjust_for_time_gap(response_data, timedelta(hours=2))

    return {"data": response_data}

# Function to adjust for time gaps
def adjust_for_time_gap(data, gap):
    
    adjusted_data = []
    last_time = None
    
    for record in data:
        created_at = record["data"]["createdAt"]
        
        if last_time is None or created_at >= last_time + gap:
            adjusted_data.append(record)
            last_time = created_at
    
    return adjusted_data

def generate_forecast_sarima(df_resampled, time_period):
    if time_period == 'day':
       forecast_periods = 1440  
       freq = '1min'  
    elif time_period == 'week':
       forecast_periods = 336  
       freq = '30min'  
    elif time_period == 'month':
       forecast_periods = 720 
       freq = '2h' 
    else:
      raise ValueError("Invalid time period. Please choose 'day', 'week', or 'month'.")

    print(f"Forecast Index Frequency: {freq}")

    if df_resampled['y'].isna().all():
        print("No actual data available. Generating default forecast.")
        forecast_index = pd.date_range(
            start=df_resampled.index[-1] + pd.Timedelta(freq),
            periods=forecast_periods,
            freq=freq,
        )
        default_value = 50  
        forecast_values = [default_value] * forecast_periods
        return pd.Series(forecast_values, index=forecast_index)

    if df_resampled['y'].notna().sum() < 48:  
        print("Insufficient data. Falling back to a default forecast.")
        forecast_values = [df_resampled['y'].iloc[-1]] * forecast_periods
        forecast_index = pd.date_range(
            start=df_resampled.index[-1] + pd.Timedelta(freq),
            periods=forecast_periods,
            freq=freq,
        )
        return pd.Series(forecast_values, index=forecast_index)

    # Fit SARIMA
    model = SARIMAX(
        df_resampled['y'],
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, 1440 if time_period == 'day' else 48 if time_period == 'week' else 12),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    model_fit = model.fit(disp=False)

    # Generate forecast
    forecast_index = pd.date_range(
        start=df_resampled.index[-1] + pd.Timedelta(freq),
        periods=forecast_periods,
        freq=freq,
    )
    forecast_values = model_fit.forecast(steps=forecast_periods)
    return pd.Series(forecast_values, index=forecast_index)
    


@TestRouter.get("/forecast")
def forecast_data(
    type: str = Query(..., description="Forecast type, e.g., forecast-kilowatt-data, forecast-current-data, forecast-voltage-data"),
    meterID: int = Query(...),
    startDate: str = Query(...),
    endDate: str = Query(...),
    timePeriod: str = Query(..., description="Forecast period, e.g., 'day', 'week', or 'month'"),
):
    try:
        
        start = parse_iso_datetime(startDate)
        end = parse_iso_datetime(endDate)
        print(f"Parsed Dates - Start: {start}, End: {end}")

        if type == "forecast-kilowatt-data":
            collection = kilowatt_collection  
        elif type == "forecast-current-data":
            collection = current_collection  
        elif type == "forecast-voltage-data":
            collection = voltage_collection  
        else:
            raise HTTPException(status_code=400, detail="Invalid forecast type. Use 'forecast-kilowatt-data', 'forecast-current-data', or 'forecast-voltage-data'.")

        if timePeriod == "week":
            start = start - timedelta(days=start.weekday() + 1)
            end = start + timedelta(days=6)
        elif timePeriod == "month":
            start = start.replace(day=1)
            end = (start.replace(month=start.month % 12 + 1, day=1) - timedelta(days=1))
        elif timePeriod == "day":
            pass
        else:
            raise HTTPException(status_code=400, detail="Invalid time period. Use 'day', 'week', or 'month'.")

        data = get_data(collection, start, end, meterID)
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found for meter {meterID} within the given time range.")

        df = pd.DataFrame(data)
        df['ds'] = pd.to_datetime(df['createdAt'])
        df['y'] = df['data'].apply(lambda x: x['value'])
        df.set_index('ds', inplace=True)

        if timePeriod == 'day':
            df_resampled = df.resample('1min').ffill()
            forecast_periods = 1440
            freq = '1min'
        elif timePeriod == 'week':
            df_resampled = df.resample('30min').ffill()
            forecast_periods = 336
            freq = '30min'
        elif timePeriod == 'month':
            df_resampled = df.resample('2h').ffill()
            forecast_periods = 720
            freq = '2h'
        else:
            raise HTTPException(status_code=400, detail="Invalid time period.")

        df_resampled['y'] = df_resampled['y'].fillna(0)

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
            raise HTTPException(status_code=500, detail=f"Error in SARIMA model fitting: {e}")

        forecast_index = pd.date_range(
            start=df_resampled.index[-1] + pd.Timedelta(freq),
            periods=forecast_periods,
            freq=freq,
        )
        forecast_values = model_fit.forecast(steps=forecast_periods)

        forecasted_data = []
        for date, value in zip(forecast_index, forecast_values):
            yhat = value + random.uniform(-0.000 * value, 0.000 * value)  
            forecasted_data.append({
                "meter": meterID,
                "data": {
                    "value": df_resampled['y'][-1] if len(df_resampled) > 0 else 0,  
                    "createdAt": date.isoformat(),
                    "yhat": yhat,
                    "ds": date.isoformat()
                }
            })

        return {"data": forecasted_data}

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
