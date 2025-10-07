import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB (make sure MongoDB is running)
client = MongoClient("mongodb://localhost:27017/")
db = client["goodbooks"]

def load_csv_to_mongo(csv_path, collection_name):
    df = pd.read_csv(csv_path)
    df = df.fillna("")  # remove NaN
    data = df.to_dict(orient="records")
    collection = db[collection_name]
    for record in data:
        if not collection.find_one(record):
            collection.insert_one(record)
    print(f"Loaded {len(data)} records into {collection_name}")

def main():
    load_csv_to_mongo("ingest/books.csv", "books")
    load_csv_to_mongo("ingest/ratings.csv", "ratings")
    load_csv_to_mongo("ingest/tags.csv", "tags")
    load_csv_to_mongo("ingest/book_tags.csv", "book_tags")
    load_csv_to_mongo("ingest/to_read.csv", "to_read")

if __name__ == "__main__":
    main()
