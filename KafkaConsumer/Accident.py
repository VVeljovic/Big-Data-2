from datetime import datetime

class Accident:
    def __init__(self, ID, Source, Severity, Start_Lat, Start_Lng,
                 End_Lat, End_Lng, Distance_mi, Description, Street, City, County,
                 State, Zipcode, Country, Timezone, Airport_Code, Weather_Timestamp,
                 Temperature_F, Wind_Chill_F, Humidity, Pressure, Visibility, Wind_Direction,
                 Wind_Speed_mph, Precipitation_in, Weather_Condition):
        
        self.ID = ID
        self.Source = Source
        self.Severity = int(Severity) if Severity else None
       # self.Start_Time = datetime.strptime(Start_Time, "%Y-%m-%d %H:%M:%S") if Start_Time else None
       # self.End_Time = datetime.strptime(End_Time, "%Y-%m-%d %H:%M:%S") if End_Time else None
        self.Start_Lat = float(Start_Lat) if Start_Lat else None
        self.Start_Lng = float(Start_Lng) if Start_Lng else None
        self.End_Lat = float(End_Lat) if End_Lat else None
        self.End_Lng = float(End_Lng) if End_Lng else None
        self.Distance_mi = float(Distance_mi) if Distance_mi else None
        self.Description = Description
        self.Street = Street
        self.City = City
        self.County = County
        self.State = State
        self.Zipcode = Zipcode
        self.Country = Country
        self.Timezone = Timezone
        self.Airport_Code = Airport_Code
        #self.Weather_Timestamp = datetime.strptime(Weather_Timestamp, "%Y-%m-%d %H:%M:%S") if Weather_Timestamp else None
        self.Temperature_F = float(Temperature_F) if Temperature_F else None
        self.Wind_Chill_F = float(Wind_Chill_F) if Wind_Chill_F else None
        self.Humidity = float(Humidity) if Humidity else None
        self.Pressure = float(Pressure) if Pressure else None
        self.Visibility = float(Visibility) if Visibility else None
        self.Wind_Direction = Wind_Direction
        self.Wind_Speed_mph = float(Wind_Speed_mph) if Wind_Speed_mph else None
        self.Precipitation_in = float(Precipitation_in) if Precipitation_in else None
        self.Weather_Condition = Weather_Condition

    def __repr__(self):
        return f"<Accident {self.ID} Severity={self.Severity} City={self.City}>"
