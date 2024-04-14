from pymongo import MongoClient
from pymongo.collection import Collection
from typing import Dict, Any, List
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

    # calls function from fill_db.py to create and fill db.
    @staticmethod
    def call_to_fill_db():
        rdb = RandDB()
        rdb.create_random_db()

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


if __name__ == "__main__":
    agg = Main()
    # agg.call_to_fill_db()
    # agg.filter1()
    # agg.filter2()
    # agg.filter3()
    # agg.filter4()
    # agg.filter5()
    # agg.filter6()




