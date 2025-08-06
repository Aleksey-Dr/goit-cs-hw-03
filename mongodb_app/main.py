import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.objectid import ObjectId

# --- MongoDB Connection Setup ---
# Replace with your MongoDB connection string.
# For local MongoDB (e.g., via Docker): "mongodb://localhost:27017/"
# For MongoDB Atlas: "mongodb+srv://<username>:<password>@<cluster-url>/<database-name>?retryWrites=true&w=majority"
MONGO_URI = "mongodb://localhost:27017/" # Default for local Docker setup
DATABASE_NAME = "cats_db"
COLLECTION_NAME = "cats"

def get_mongo_collection():
    """
    Establishes a connection to MongoDB and returns the collection object.
    Handles connection errors.
    """
    try:
        client = pymongo.MongoClient(MONGO_URI)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print("Successfully connected to MongoDB.")
        return collection
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
        print("Please ensure MongoDB is running and your MONGO_URI is correct.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}")
        return None

# --- CRUD Operations ---

def create_cat(collection, name, age, features):
    """
    Inserts a new cat document into the collection.
    """
    if not collection: return
    try:
        cat_data = {
            "name": name,
            "age": age,
            "features": features
        }
        result = collection.insert_one(cat_data)
        print(f"Cat '{name}' added with ID: {result.inserted_id}")
        return result.inserted_id
    except OperationFailure as e:
        print(f"Error inserting cat '{name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while creating cat '{name}': {e}")

def read_all_cats(collection):
    """
    Retrieves and prints all documents from the collection.
    """
    if not collection: return
    try:
        print("\n--- All Cats in Collection ---")
        cats = collection.find({})
        found_cats = False
        for cat in cats:
            print(f"ID: {cat.get('_id')}, Name: {cat.get('name')}, Age: {cat.get('age')}, Features: {cat.get('features')}")
            found_cats = True
        if not found_cats:
            print("No cats found in the collection.")
        print("----------------------------")
        return list(cats) # Return as a list for potential further processing
    except OperationFailure as e:
        print(f"Error reading all cats: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while reading all cats: {e}")
    return []

def read_cat_by_name(collection, cat_name):
    """
    Retrieves and prints information about a specific cat by its name.
    """
    if not collection: return
    try:
        cat = collection.find_one({"name": cat_name})
        if cat:
            print(f"\n--- Cat Found: {cat_name} ---")
            print(f"ID: {cat.get('_id')}, Name: {cat.get('name')}, Age: {cat.get('age')}, Features: {cat.get('features')}")
            print("----------------------------")
            return cat
        else:
            print(f"Cat '{cat_name}' not found.")
            return None
    except OperationFailure as e:
        print(f"Error reading cat '{cat_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while reading cat '{cat_name}': {e}")
    return None

def update_cat_age(collection, cat_name, new_age):
    """
    Updates the age of a cat identified by its name.
    """
    if not collection: return
    try:
        result = collection.update_one(
            {"name": cat_name},
            {"$set": {"age": new_age}}
        )
        if result.matched_count > 0:
            print(f"Cat '{cat_name}' age updated to {new_age}.")
        else:
            print(f"Cat '{cat_name}' not found for age update.")
        return result.matched_count > 0
    except OperationFailure as e:
        print(f"Error updating age for cat '{cat_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while updating cat '{cat_name}'s age: {e}")
    return False

def add_cat_feature(collection, cat_name, new_feature):
    """
    Adds a new feature to the 'features' list of a cat identified by its name.
    """
    if not collection: return
    try:
        result = collection.update_one(
            {"name": cat_name},
            {"$addToSet": {"features": new_feature}} # $addToSet prevents duplicate features
        )
        if result.matched_count > 0:
            print(f"Feature '{new_feature}' added to cat '{cat_name}'.")
        else:
            print(f"Cat '{cat_name}' not found for feature update.")
        return result.matched_count > 0
    except OperationFailure as e:
        print(f"Error adding feature to cat '{cat_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while adding feature to cat '{cat_name}': {e}")
    return False

def delete_cat_by_name(collection, cat_name):
    """
    Deletes a single cat document from the collection by its name.
    """
    if not collection: return
    try:
        result = collection.delete_one({"name": cat_name})
        if result.deleted_count > 0:
            print(f"Cat '{cat_name}' deleted successfully.")
        else:
            print(f"Cat '{cat_name}' not found for deletion.")
        return result.deleted_count > 0
    except OperationFailure as e:
        print(f"Error deleting cat '{cat_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while deleting cat '{cat_name}': {e}")
    return False

def delete_all_cats(collection):
    """
    Deletes all documents from the collection.
    """
    if not collection: return
    try:
        confirm = input("Are you sure you want to delete ALL cats? (yes/no): ").lower()
        if confirm == 'yes':
            result = collection.delete_many({})
            print(f"Deleted {result.deleted_count} cats from the collection.")
            return result.deleted_count
        else:
            print("Deletion cancelled.")
            return 0
    except OperationFailure as e:
        print(f"Error deleting all cats: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while deleting all cats: {e}")
    return 0

# --- Main Execution Block ---
if __name__ == "__main__":
    collection = get_mongo_collection()

    if collection:
        print("\n--- Demonstrating CRUD Operations ---")

        # 1. Create (Insert)
        print("\n--- Creating Cats ---")
        create_cat(collection, "Musya", 5, ["sleeps a lot", "loves fish", "tricolor"])
        create_cat(collection, "Busya", 4, ["affectionate", "loves to eat", "tricolor"])
        create_cat(collection, "Abrykoska", 3, ["brave", "likes to be petted", "red with white"])
        create_cat(collection, "Sara", 2, ["plays with toys", "very active", "smart"])

        # 2. Read All
        read_all_cats(collection)

        # 3. Read by Name
        cat_to_find = input("\nEnter the name of the cat to find: ")
        read_cat_by_name(collection, cat_to_find)

        # 4. Update Age
        cat_to_update_age = input("\nEnter the name of the cat to update age: ")
        new_age_str = input(f"Enter the new age for {cat_to_update_age}: ")
        try:
            new_age = int(new_age_str)
            update_cat_age(collection, cat_to_update_age, new_age)
        except ValueError:
            print("Invalid age entered. Please enter a number.")

        # 5. Add Feature
        cat_to_add_feature = input("\nEnter the name of the cat to add a feature: ")
        new_feature = input(f"Enter the new feature for {cat_to_add_feature}: ")
        add_cat_feature(collection, cat_to_add_feature, new_feature)

        # Show updated state
        read_all_cats(collection)

        # 6. Delete by Name
        cat_to_delete = input("\nEnter the name of the cat to delete: ")
        delete_cat_by_name(collection, cat_to_delete)

        # Show state after single deletion
        read_all_cats(collection)

        # 7. Delete All (with confirmation)
        delete_all_cats(collection)

        # Show final state
        read_all_cats(collection)
    else:
        print("Skipping CRUD operations due to MongoDB connection failure.")

