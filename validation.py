from datetime import date


def create_rules(fill):
    validation_rules = {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": [fill.str_field1, fill.str_field2, fill.str_date, fill.int_field, fill.float_field],
                "properties": {
                    fill.str_field1: {
                        "bsonType": "string",
                        "minLength": 2,
                        "maxLength": 10
                    },
                    fill.str_field2: {
                        "bsonType": "string",
                        "minLength": 2,
                        "maxLength": 10
                    },
                    fill.str_date: {
                        "bsonType": "string",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
                    },
                    fill.int_field: {
                        "bsonType": "int"
                    },
                    fill.float_field: {
                        "bsonType": "double",
                        "minimum": 0
                    }
                }
            }
        }
    }
    return validation_rules


def validate_date(date_text):
    try:
        date.fromisoformat(date_text)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


# # validates new collection upon creation
# db.create_collection(collection.name, validator=validation_rules['validator'])

# # validates collection when adding additional data
# db.command("collMod", collection.name, **validation_rules)

if __name__ == "__main__":
    pass
