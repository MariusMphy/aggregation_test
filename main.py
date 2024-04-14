from pymongo import MongoClient
from pymongo.collection import Collection
from typing import Dict, Any, List
from datetime import datetime
from pymongo.cursor import Cursor
from pprint import pprint
from pymongo.errors import OperationFailure, WriteError, ConfigurationError, PyMongoError
import random
from fill_db import RandDB


class Main:
    def __init__(self):
        client = MongoClient("localhost", 27017)
        db = client["db_aggregation"]
        self.agg1 = db["agg1"]
        self.agg2 = db["agg2"]
        self.agg3 = db["agg3"]

        self.enter_cars = None
        self.enter_owners = None
        self.cons_min = None
        self.cons_max = None
        self.mileage_min = None
        self.mileage_max = None
        self.date_min = None
        self.date_max = None

    # calls function from fill_db.py to create and fill db.
    @staticmethod
    def call_to_fill_db():
        rdb = RandDB()
        rdb.create_random_db()

    # tried to use pipeline as separate function, but it seems it is better not
    @staticmethod
    def pipeline(collection: Collection, filter_criteria: list):
        pipeline = [
            {
                "$match": {
                    "$and": filter_criteria
                },
            },
        ]

        return collection.aggregate(pipeline)

    # filter collection
    def filter1(self):
        filter_criteria: List[Dict[str, Any]] = [
            {'Animal': 'Cat'},
            {'$or': [{'Owner': 'Anthony'}, {'Owner': "William"}]},
            {'Weight': {'$not': {'$gt': 40}}}
        ]
        result_cursor = self.pipeline(self.agg1, filter_criteria)

        for document in result_cursor:
            print(document)

    # Filter and sort
    def filter2(self):
        filter_criteria: List[Dict[str, Any]] = [
            {'Product': 'Speakers'},
            {'$or': [{'Category': 'z'}, {'Category': "m"}]},
            {'Weight': {'$not': {'$gt': 55}}}
        ]

        sort_criteria = {"Category": 1, "Quantity": -1}

        pipeline = [
            {
                "$match": {
                    "$and": filter_criteria
                },
            },
            {
                '$sort': sort_criteria
            }
        ]
        result_cursor = self.agg2.aggregate(pipeline)

        for document in result_cursor:
            print(document)

    # Filter, sort and project
    def filter3(self):
        filter_criteria: List[Dict[str, Any]] = [
            {'Product': 'Speakers'},
            {'$or': [{'Category': 'z'}, {'Category': "m"}]},
            {'Weight': {'$not': {'$gt': 55}}}
        ]

        sort_criteria = {"Category": -1, "Quantity": 1}
        projecting_criteria = {"_id": 0, "Product": 1, "Production date": 1, "Quantity": 1, "Category": 1}

        pipeline = [
            {
                "$match": {
                    "$and": filter_criteria
                },
            },
            {
                '$sort': sort_criteria
            },
            {
                '$project': projecting_criteria
            }
        ]
        result_cursor = self.agg2.aggregate(pipeline)

        for document in result_cursor:
            print(document)

    # Filter, group, project and sort
    def filter4(self):
        filter_criteria: List[Dict[str, Any]] = [
            {'Product': 'Speakers'},
            {'$or': [{'Category': 'z'}, {'Category': "m"}, {'Category': "v"}]},
            {'Weight': {'$not': {'$gt': 55}}}
        ]

        group_criteria = {"_id": "$Category", "count": {"$sum": 1}, "Average Weight": {"$avg": "$Weight"}}
        projecting_criteria = {"_id": 0, "count": 1, "Average Weight": 1}
        sort_criteria = {"count": -1, "Average Weight": 1}

        pipeline = [
            {
                "$match": {
                    "$and": filter_criteria
                },
            },
            {
                '$group': group_criteria
            },
            {
                '$project': projecting_criteria
            },
            {
                '$sort': sort_criteria
            }
        ]
        result_cursor = self.agg2.aggregate(pipeline)

        for document in result_cursor:
            print(document)

    # group, round, project and sort
    def filter5(self):
        group_criteria = {"_id": "$Animal", "count": {"$sum": 1}, "Average Height": {"$avg": "$Height"}}
        project_criteria = {"Animal": "$_id", "count": 1, "Average Height": 1, "_id": 0}
        sort_criteria = {"count": -1}

        pipeline = [{'$group': group_criteria},
                    {"$addFields": {"Average Height": {"$round": ["$Average Height", 2]}}},  # rounding height
                    {"$project": project_criteria},
                    {"$sort": sort_criteria}]

        result_cursor = self.agg1.aggregate(pipeline)

        for document in result_cursor:
            print(document)

    # filter and group
    def filter6(self):
        filter_criteria = [{"$and": [{"Animal": "Cat"}, {"Weight": {"$gt": 10}}]}]
        group_criteria = {"_id": "$Animal", "count": {"$sum": 1}, "Average Height": {"$avg": "$Height"}}
        pipeline = [{'$match': {"$and": filter_criteria}}, {'$group': group_criteria}]
        result_cursor = self.agg1.aggregate(pipeline)

        for document in result_cursor:
            print(document)

    # User input for agg3 collection. Enter for default values.
    def agg3_input(self):
        cars = self.agg3.distinct("Car", {"Car": {"$exists": True}})
        print(", ".join(cars))
        self.enter_cars = input(f"Enter cars from the list above, separated by comma, case sensitive: ")
        owners = self.agg3.distinct("Owner", {"Owner": {"$exists": True}})
        print(", ".join(owners))
        self.enter_owners = input(f"Enter owners from the list above, separated by comma, case sensitive: ")
        self.cons_min = input("Enter minimum fuel consumption: ")
        self.cons_max = input("Enter maximum fuel consumption: ")
        self.mileage_min = input("Enter minimum mileage: ")
        self.mileage_max = input("Enter maximum mileage: ")
        self.date_min = input("Enter minimum date of production (YYYY-MM-DD): ")
        self.date_max = input("Enter maximum date of production (YYYY-MM-DD): ")

    # filters by user input
    def filter7(self):
        self.agg3_input()
        enter_cars = self.enter_cars.split(", ")
        enter_owners = self.enter_owners.split(", ")
        cons_min = 0 if self.cons_min == "" else float(self.cons_min)
        cons_max = float("inf") if self.cons_max == "" else float(self.cons_max)
        mileage_min = 0 if self.mileage_min == "" else int(self.mileage_min)
        mileage_max = 0 if self.mileage_max == "" else int(self.mileage_max)
        date_min = self.date_min
        date_max = self.date_max
        car_filter = {} if len(enter_cars) == 1 and enter_cars[0] == "" else {"Car": {"$in": enter_cars}}
        owner_filter = {} if len(enter_owners) == 1 and enter_owners[0] == "" else {"Owner": {"$in": enter_owners}}

        consumption_filter = {"Fuel consumption": {"$gte": 0 if not cons_min else cons_min,
                                                   "$lte": float("inf") if not cons_max else cons_max}}
        mileage_filter = {"Mileage": {"$gte": 0 if not mileage_min else mileage_min,
                                      "$lte": float("inf") if not mileage_max else mileage_max}}
        date_filter = {"Prod date": {"$gte": "1900-00-00" if not date_min else date_min,
                                     "$lte": datetime.today().strftime("%Y-%m-%d") if not date_max else date_max}}

        filter_criteria = [car_filter, owner_filter, consumption_filter, mileage_filter, date_filter]

        pipeline = [
            {
                "$match": {"$and": filter_criteria}
            }
        ]
        result_cursor = self.agg3.aggregate(pipeline)

        for document in result_cursor:
            print(document)


if __name__ == "__main__":
    agg = Main()
    # agg.call_to_fill_db()
    # agg.filter1()
    # agg.filter2()
    # agg.filter3()
    # agg.filter4()
    # agg.filter5()
    # agg.filter6()
    # agg.agg3_input()
    agg.filter7()

    # pprint(list(agg.agg3.find_one()))





