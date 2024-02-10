# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import datetime

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine(f"sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Stations = Base.classes.station
Measurements = Base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################

# Start at the homepage and list all the available routes.
@app.route("/")
def home():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )
    
# Convert the query results from your precipitation analysis (i.e. retrieve only the last 12 months of data) to a dictionary using date as the key and prcp as the value. 
# Return the JSON representation of your dictionary.

@app.route("/api/v1.0/precipitation")
def precipitation():
    recent_date_query = session.query(func.max(Measurements.date)).scalar()
    recent_date = pd.to_datetime(recent_date_query)
    one_year = recent_date - pd.DateOffset(years=1)
    one_year_ago = one_year.strftime('%Y-%m-%d') 
    results = session.query(Measurements.date, Measurements.prcp).filter(Measurements.date >= one_year_ago).all()
    precipitation_data = {date: prcp for date, prcp in results}
    session.close()
    return jsonify(precipitation_data)


# Return a JSON list of stations from the dataset.
@app.route("/api/v1.0/stations")
def stations():
    stations = session.query(Measurements.station).all()
    station_list = [station[0] for station in stations]
    session.close()
    return jsonify(station_list)


# Query the dates and temperature observations of the most-active station for the previous year of data.
# Return a JSON list of temperature observations for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():
    station_activity = session.query(Measurements.station, func.count(Measurements.station)).group_by(Measurements.station).order_by(func.count(Measurements.station).desc()).first()[0]
    recent_date_query = session.query(func.max(Measurements.date)).scalar()
    recent_date = pd.to_datetime(recent_date_query)
    one_year = recent_date - pd.DateOffset(years=1)
    one_year_ago = one_year.strftime('%Y-%m-%d') 
    results = session.query(Measurements.date, Measurements.tobs).filter(Measurements.date >= one_year_ago).filter(Measurements.station == station_activity).all()
    tobs_data = [{'Date': date, 'Temperature': tobs} for date, tobs in results]
    session.close()
    return jsonify(tobs_data)
    
    
# Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range.
# For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.
# For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.
@app.route("/api/v1.0/temp/<start>")
@app.route("/api/v1.0/temp/<start>/<end>")
def stats(start=None, end=None):
    """Return TMIN, TAVG, TMAX."""
    # Select statement
    sel = [func.min(Measurements.tobs), func.avg(Measurements.tobs), func.max(Measurements.tobs)]
    if not end:
        start = dt.datetime.strptime(start, "%m%d%Y")
        results = session.query(*sel).\
            filter(Measurements.date >= start).all()
        session.close()
        temps = list(np.ravel(results))
        return jsonify(temps)
    # calculate TMIN, TAVG, TMAX with start and stop
    start = dt.datetime.strptime(start, "%m%d%Y")
    end = dt.datetime.strptime(end, "%m%d%Y")
    results = session.query(*sel).\
        filter(Measurements.date >= start).\
        filter(Measurements.date <= end).all()
    session.close()
    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(results))
    return jsonify(temps=temps)

if __name__ == '__main__':
    app.run()
