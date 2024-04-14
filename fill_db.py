from pymongo import MongoClient
import random
from datetime import datetime, timedelta


class RandDB:
    def __init__(self):
        pass

    @staticmethod
    def randomize_word(library):
        with open(f'{library}.txt', 'r') as file:
            # Read the contents of the file
            contents = file.read()
        # Split the text into individual words
        word = contents.split()
        # return random word from the list
        return random.choice(word)

    @ staticmethod
    def random_date(start_date, end_date):
        # Convert the string dates to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Calculate the range in days
        delta = end_date - start_date

        # Generate a random number of days within the range
        random_days = random.randint(0, delta.days)

        # Add the random number of days to the start date
        random_date = start_date + timedelta(days=random_days)

        # Return the random date as a string in the format 'yyyy-mm-dd'
        return random_date.strftime("%Y-%m-%d")

    def create_random_db(self):
        db_name = input("Enter DB name: ")
        collection_name = input("Enter collection name: ")
        amount = int(input("Enter amount of documents in collection: "))
        # will maybe implement later option to pick number of every type (str, int, float) of field.
        # str_fields_no = int(input("Enter how many string fields you want: "))
        str_field1_library = input("Chose string field 1 from (animals5, animals20, cars5, cars20, "
                                   "electronics5, electronics20, letters5, letters20, names5, names20, names200"
                                   "words5, words20, words213)")
        str_field1 = input("Enter name for string field 1: ")
        str_field2_library = input("Chose string field 1 from (animals5, animals20, cars5, cars20, "
                                   "electronics5, electronics20, letters5, letters20, names5, names20, names200"
                                   "words5, words20, words213)")
        str_field2 = input("Enter name of string field 2: ")
        str_date = input("Enter date field name: ")
        starting_date = input("Enter starting date for date field (yyyy-mm-dd): ")
        int_field = input("Enter name of integer field: ")
        int_start = int(input("Enter starting number of integer field range: "))
        int_end = int(input("Enter ending number of integer field range: "))
        float_field = input("Enter name of float field: ")
        float_start = round(float(input("Enter starting number of float field range: ")), 4)
        float_end = round(float(input("Enter ending number of float field range: ")), 4)
        str_today = datetime.today().strftime("%Y-%m-%d")
        client = MongoClient("localhost", 27017)
        db = client[db_name]
        collection = db[collection_name]
        for number in range(amount):
            goods = {
                str_field1: self.randomize_word(str_field1_library),
                str_field2: self.randomize_word(str_field2_library),
                str_date: self.random_date(starting_date, str_today),
                int_field: random.randint(int_start, int_end),
                float_field: round(random.uniform(float_start, float_end), 4)
            }
            collection.insert_one(goods)


if __name__ == "__main__":
    rdb = RandDB()
    rdb.create_random_db()
