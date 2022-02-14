from sqlalchemy import create_engine
import pandas as pd
import pyodbc

data = pd.read_csv(r'E:\project\winter\bigdata\assingment2\austin_weather.csv')
df = pd.DataFrame(data)

print(df)

cnxn = pyodbc.connect("Trusted_Connection=yes",
                       driver="{SQL Server}", server="localhost",
                       uid="mehd!", pwd="007@1744185", database="schooldb")
cursor = cnxn.cursor()

cursor.execute('''
		CREATE TABLE austinWeather (
			product_id int primary key,
			date date,
			TempHighF decimal,
            TempAvgF decimal,
            TempLowF decimal,
            DewPointHighF decimal,
            DewPointAvgF decimal,
            DewPointLowF decimal,
            HumidityHighPercent decimal,
            HumidityAvgPercent decimal,
            HumidityLowPercent decimal,
            SeaLevelPressureHighInches decimal,
            SeaLevelPressureAvgInches decimal,
            SeaLevelPressureLowInches decimal,
            VisibilityHighMiles decimal,
            VisibilityAvgMiles decimal,
            VisibilityLowMiles decimal,
            WindHighMPH decimal,
            WindAvgMPH decimal,
            WindGustMPH decimal,
            PrecipitationSumInches character,
            Events nvarchar(60)
			            )
               ''')

# Insert DataFrame to Table
for row in df.itertuples():
    cursor.execute('''
                INSERT INTO products (Date, TempHighF, TempAvgF, TempLowF,
                	                  DewPointHighF, DewPointAvgF,DewPointLowF,
                	                  HumidityHighPercent, HumidityAvgPercent,
                	                  HumidityLowPercent, SeaLevelPressureHighInches,
                	                  SeaLevelPressureAvgInches, SeaLevelPressureLowInches,
                	                  VisibilityHighMiles, VisibilityAvgMiles,
                	                  VisibilityLowMiles, WindHighMPH, WindAvgMPH,
                	                  WindGustMPH, PrecipitationSumInches, Events)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
                   row.Date,
                   row.TempHighF,
                   row.TempAvgF,
                   row.TempLowF,
                   row.DewPointHighF,
                   row.DewPointAvgF,
                   row.DewPointLowF,
                   row.HumidityHighPercent,
                   row.HumidityAvgPercent,
                   row.HumidityLowPercent,
                   row.SeaLevelPressureHighInches,
                   row.SeaLevelPressureAvgInches,
                   row.SeaLevelPressureLowInches,
                   row.VisibilityHighMiles,
                   row.VisibilityAvgMiles,
                   row.VisibilityLowMiles,
                   row.WindHighMPH,
                   row.WindAvgMPH,
                   row.WindGustMPH,
                   row.PrecipitationSumInches,
                   row.Events
                   )
cnxn.commit()

cursor.execute('SELECT * FROM austinWeather')

for row in cursor:
    print('row = %r' % (row,))

cnxn.close()