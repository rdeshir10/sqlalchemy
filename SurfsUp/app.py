# Import the dependencies.
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

Base.prepare(autoload_with=engine)

# reflect the tables
Base.classes.keys()

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB

session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# Create base to list all APIs
@app.route("/")
def names():
    return (f"Welcome to my SQL Alchemy Challenge page<br>"
    f"Here is a list of the API's for this challenge<br>"
    f"1) /api/v1.0/precipitation<br>"
    f"2) /api/v1.0/stations<br>"
    f"3) /api/v1.0/tobs<br>"
    f"4) /api/v1.0/startdate/<start><br>"
    f"5) /api/v1.0/startdate/<start>/enddate/<end>")

# Api for precipitation
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create Session to connect to database
    session = Session(engine)

    # Calculate the date one year from the last date in data set.

    past_year = session.query(func.date(func.max(measurement.date), '-1 year')).scalar()

    # Perform a query to retrieve the data and precipitation scores

    results = session.query(measurement.date, measurement.prcp).filter(measurement.date >= past_year).all()

    session.close()

    data_prcp_list = []

    for date, prcp in results:
        data_prcp = {}
        data_prcp[date] = prcp
        data_prcp_list.append(data_prcp)

    title = "Dates and Precipitation list is as follows"
    # Converting Data to Json
    return jsonify(title, data_prcp_list)

# Api for list of stations
@app.route("/api/v1.0/stations")
def stations():
    # Create Session to connect to database
    session = Session(engine)

    # Query to retrieve list of all stations
    results = session.query(station.station).all()
    station_list = list(np.ravel(results))

    session.close()

    title = "List of stations is as follows"
    # Converting Data to Json
    return jsonify(title, station_list)

@app.route("/api/v1.0/tobs")
def tobs():
     # Create Session to connect to database
    session = Session(engine)

    # Query to find the most active station
    most_active_station = session.query(measurement.station).\
    group_by(measurement.station).order_by(func.count(measurement.station).desc()).first()[0]
    
    # Query to find the dates and temperatures of the most active station
    date_temp = session.query(measurement.date, measurement.tobs).filter(measurement.station == most_active_station).all()

    # Closing session
    session.close()

    # Using List comprehension to populate list. 
    list_comp = [{"Date" : date , "Temperature" : temp} for date,temp in date_temp]
    
    title = f"Information for the most active station : {most_active_station}"

    # Converting Data to Json
    return jsonify(title,list_comp)

@app.route("/api/v1.0/startdate/<start>")
def start_date(start):
     # Create Session to connect to database
    session = Session(engine)

    # Query to find the min, avg and max temp for a sepcified start date
    results = session.query(measurement.date, func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= start).\
        group_by(measurement.date).all()
    
    session.close()
    
    # List comprehension to generate list of dates and Tmin,Tavg and Tmax for specified date onwards.
    temp_dict = [{"Date": date,
                  "Minimum Temperature":min,
                  "Average Temperature":avg,
                  "Maximum Temperature": max} for date,min,avg,max in results]

    title = f"The temperature information for the dates after {start} is as follows"
    # Converting Data to Json
    return jsonify(title,temp_dict)

@app.route("/api/v1.0/startdate/<start>/enddate/<end>")
def start_end(start,end):
    # Create Session to connect to database
    session = Session(engine)
    
    # Query to find the min, avg and max temp for sepcified start and end dates.
    results = session.query(measurement.date, func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= start).\
        filter(measurement.date <= end).\
        group_by(measurement.date).all()

    session.close()

    # List comprehension to generate list of dates and Tmin,Tavg and Tmax between specified start and end dates.
    temp_dict = [{"Date": date,
                  "Minimum Temperature":min,
                  "Average Temperature":avg,
                  "Maximum Temperature": max} for date,min,avg,max in results]
    
    title = f"The temperature information for the date range {start} and {end} is as follows"
    # Converting Data to Json
    return jsonify(title,temp_dict)

if __name__ == '__main__':
    app.run(debug=True)