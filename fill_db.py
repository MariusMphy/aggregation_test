from pymongo import MongoClient
import random
from datetime import datetime, timedelta
from pymongo.errors import OperationFailure, WriteError, ConfigurationError, PyMongoError
from pprint import pprint
import validation


class RandDB:
    def __init__(self):
        self.db_name = None
        self.collection_name = None
        self.amount = None
        self.str_field1 = None
        self.str_field2 = None
        self.str_date = None
        self.int_field = None
        self.float_field = None

    @staticmethod
    def randomize_word(library):
        with open(f'libraries\\{library}.txt', 'r') as file:
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

    def create_empty_collection(self):
        self.db_name = input("Enter DB name: ")
        self.collection_name = input("Enter collection name: ")
        self.amount = int(input("Enter amount of documents in collection: "))
        self.str_field1 = input("Enter name for string field 1: ")
        self.str_field2 = input("Enter name of string field 2: ")
        self.str_date = input("Enter date field name: ")
        self.int_field = input("Enter name of integer field: ")
        self.float_field = input("Enter name of float field: ")

    def create_random_db(self):
        self.create_empty_collection()
        str_field1_library = input("Chose string field 1 from (animals5, animals20, cars5, cars20, "
                                   "electronics5, electronics20, letters5, letters20, names5, names20, names200"
                                   "words5, words20, words213): ")
        str_field2_library = input("Chose string field 2 from (animals5, animals20, cars5, cars20, "
                                   "electronics5, electronics20, letters5, letters20, names5, names20, names200"
                                   "words5, words20, words213): ")
        starting_date = input("Enter starting date for date field (yyyy-mm-dd): ")
        ending_date = input("Enter ending date for date field (yyyy-mm-dd) or press enter for today: ")
        str_today = datetime.today().strftime("%Y-%m-%d")
        ending_confirmed = str_today if not ending_date else ending_date
        int_start = int(input("Enter starting number of integer field range: "))
        int_end = int(input("Enter ending number of integer field range: "))
        float_start = round(float(input("Enter starting number of float field range: ")), 4)
        float_end = round(float(input("Enter ending number of float field range: ")), 4)

        client = MongoClient("localhost", 27017)
        db = client[self.db_name]
        collection = db[self.collection_name]
        # validate new data
        validation_rules = validation.create_rules(self)
        db.create_collection(collection.name, validator=validation_rules['validator'])

        for number in range(self.amount):
            goods = {
                self.str_field1: self.randomize_word(str_field1_library),
                self.str_field2: self.randomize_word(str_field2_library),
                self.str_date: self.random_date(starting_date, ending_confirmed),
                self.int_field: random.randint(int_start, int_end),
                self.float_field: round(random.uniform(float_start, float_end), 4)
            }
            try:
                collection.insert_one(goods)
            except WriteError as wr:
                pprint(wr.details)
            except OperationFailure as op:
                pprint(op)

            # collection.insert_one(goods)


if __name__ == "__main__":
    rdb = RandDB()
    rdb.create_random_db()
