
import sqlite3
import pymongo

# SQLite connection
connection = sqlite3.connect("SolarEclipses.db")
cursor = connection.cursor()

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://amysaad98:8876873333@gradebook.gisyiki.mongodb.net/?retryWrites=true&w=majority&appName=gradebook")
database = client["solar_eclipse_data"]
boston_collection = database["boston"]
boston_collection = database["providence"]
boston_collection = database["albany"]

cursor.execute("PRAGMA table_info(boston)")
columns = [col[1] for col in cursor.fetchall()]

# Extract all rows from boston table
cursor.execute("SELECT * FROM boston")
rows = cursor.fetchall()

cursor.execute("SELECT * FROM providence")
rows = cursor.fetchall()

cursor.execute("SELECT * FROM albany")
rows = cursor.fetchall()

documents = []
for row in rows:
    doc = {}
    for i, value in enumerate(row):
        doc[columns[i]] = value
    documents.append(doc)
    
collection = database["boston"]
collection.insert_many(documents)    

collection = database["providence"]
collection.insert_many(documents)   

collection = database["albany"]
collection.insert_many(documents)   

user_input = input("Which collection do you want to view? (enter boston, providence, or albany): ")
collection = database[user_input]

    
total_eclipses = collection.count_documents({"Eclipse_Type": "T"})
print("Number of Total Solar Eclipses occurring this century:", total_eclipses)
    
    # Step 8: Local time when the next maximum eclipse occurs
max_eclipse = collection.find_one({"Calendar_Date": "2079-May-01"}, {"_id": 0, "Maximum_Eclipse": 1})
print(f"Local time when the eclipse reaches its maximum: {max_eclipse['Maximum_Eclipse']}")
    
    # Total duration of the total eclipse
duration = collection.find_one({"Calendar_Date": "2079-May-01"}, {"_id": 0, "A_or_T_Eclipse_Duration": 1})
print(f"Total duration: {duration['A_or_T_Eclipse_Duration']}")
    
    # Step 9: Unique query
unique_query = {"A_or_T_Eclipse_Duration": {"$gt": "3m"}}
for doc in collection.find(unique_query):
    print(doc)
