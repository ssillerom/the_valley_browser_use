import pymongo
from pymongo.errors import ConnectionFailure

def crud_operations():
    """
    Demonstrates CRUD operations using PyMongo.
    Assumes a MongoDB instance is running on localhost:27017.
    """
    client = None
    try:
        # 1. Connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        
        # The ismaster command is cheap and does not require auth.
        client.admin.command('''ismaster''')
        print("MongoDB connection successful!")

        # 2. Select a database and a collection
        db = client["mydatabase"]
        collection = db["mycollection"]

        # --- C (Create) Operations ---
        print("\n--- Creating Documents ---")

        # Insert a single document
        document1 = {"name": "Alice", "age": 30, "city": "New York"}
        insert_one_result = collection.insert_one(document1)
        print(f"Inserted single document with ID: {insert_one_result.inserted_id}")

        # Insert multiple documents
        documents = [
            {"name": "Bob", "age": 24, "city": "London"},
            {"name": "Charlie", "age": 35, "city": "Paris"},
            {"name": "David", "age": 28, "city": "New York"}
        ]
        insert_many_result = collection.insert_many(documents)
        print(f"Inserted multiple documents with IDs: {insert_many_result.inserted_ids}")

        # --- R (Read) Operations ---
        print("\n--- Reading Documents ---")

        # Find all documents
        print("\nAll documents in collection:")
        for doc in collection.find():
            print(doc)

        # Find documents with a specific criterion
        print("\nDocuments where city is '''New York''':")
        for doc in collection.find({"city": "New York"}):
            print(doc)

        # Find one document
        print("\nOne document where age is 24:")
        one_doc = collection.find_one({"age": 24})
        print(one_doc)
        
        # Find with projection (return only specific fields)
        print("\nDocuments showing only name and city:")
        for doc in collection.find({}, {"name": 1, "city": 1, "_id": 0}): # _id: 0 to exclude the default _id field
            print(doc)

        # --- U (Update) Operations ---
        print("\n--- Updating Documents ---")

        # Update a single document
        update_one_result = collection.update_one(
            {"name": "Alice"},
            {"$set": {"age": 31, "status": "updated"}}
        )
        print(f"Matched {update_one_result.matched_count} document(s) and modified {update_one_result.modified_count} document(s) for Alice.")
        print("Updated Alice'''s document:", collection.find_one({"name": "Alice"}))

        # Update multiple documents
        update_many_result = collection.update_many(
            {"city": "New York"},
            {"$set": {"is_usa": True}}
        )
        print(f"Matched {update_many_result.matched_count} document(s) and modified {update_many_result.modified_count} document(s) for New York residents.")
        print("Documents where city is '''New York''' after batch update:")
        for doc in collection.find({"city": "New York"}):
            print(doc)
            
        # Add a new field if it doesn'''t exist, or update if it does (upsert)
        update_upsert_result = collection.update_one(
            {"name": "Eve"}, # Document "Eve" does not exist
            {"$set": {"age": 29, "country": "Canada"}},
            upsert=True
        )
        print(f"Upserted document with ID: {update_upsert_result.upserted_id if update_upsert_result.upserted_id else '''N/A'''}")
        print("Eve'''s document:", collection.find_one({"name": "Eve"}))

        # --- D (Delete) Operations ---
        print("\n--- Deleting Documents ---")

        # Delete a single document
        delete_one_result = collection.delete_one({"name": "Bob"})
        print(f"Deleted {delete_one_result.deleted_count} document(s) for Bob.")
        print("Documents remaining after deleting Bob:")
        for doc in collection.find():
            print(doc)

        # Delete multiple documents
        delete_many_result = collection.delete_many({"city": "Paris"})
        print(f"Deleted {delete_many_result.deleted_count} document(s) where city is Paris.")
        print("Documents remaining after deleting Paris residents:")
        for doc in collection.find():
            print(doc)

        # Optional: Delete the entire collection (use with caution!)
        # collection.drop()
        # print("\nCollection '''mycollection''' dropped.")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
        print("Please ensure MongoDB is running on localhost:27017.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if client:
            client.close()
            print("\nMongoDB connection closed.")

if __name__ == "__main__":
    crud_operations()
