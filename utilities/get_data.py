import json
import logging
from .helper_psql import insert_member


def get_selfdev_members(psql_conn):
    """ Read Members from data/selfdev_members_enriched.json and insert them into the database"""
    with open('data/selfdev_members_enriched.json') as json_file:
        data = json.load(json_file)
        for member in data:
            logging.info(f"Inserting member {member['name']}")
            insert_member(psql_conn, member)
            