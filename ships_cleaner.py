import pymongo
import matplotlib.pyplot as plt
import sys
import plotly.plotly as py
import datetime
import dateutil.parser
import time
from util_functions import parse_property_key, turn_property_to_array


# recieving from user score cutoff
# in the future we can work either with: a GUI, WEB GUI, questions and pop ups, or with a config file
try:
    score_cutoff = float(sys.argv[1])
except IndexError:
    score_cutoff = 0

#connecting to client
client = pymongo.MongoClient('mongodb://localhost:27017/')

#creating the new work db for the session
name_list = client.database_names()
if 'working_db' not in name_list:
    client.admin.command('copydb', fromdb='planet', todb='working_db', upsert=True)
else:
    client.drop_database('working_db')
    client.admin.command('copydb', fromdb='planet', todb='working_db', upsert=True)

db = client.working_db
score_query = {"$or": [{"properties.score": {"$lt": score_cutoff}}, {"properties.cloud_cover": 1}]}
ships_collection = db.ships
response = ships_collection.delete_many(score_query)
print(str(response.deleted_count) + " ships were deleted, as they were either covered by clouds or had a low score")

#create new properties for the entries: satellite name and time
projection = {"geometry": 1, "properties.source_item": 1,
              "properties.observed": 1, "id": 1}
all_ships = ships_collection.find({}, projection)
iteration = 0
for ship in all_ships:
    print(iteration)
    iteration += 1
    current_satellite = ship["properties"]["source_item"].split('_')
    currentTimeTuple = dateutil.parser.parse(ship["properties"]["observed"]).timetuple()
    ships_collection.update_one({"_id": ship["_id"]}, {"$set": {"time_in_seconds": time.mktime(currentTimeTuple)}})
    ships_collection.update_one({"_id": ship["_id"]}, {"$set": {"satellite_name": current_satellite[2]}})

cursor2 = ships_collection.find({"satellite_name": {'$exists': True}}, {"satellite_name": 1,
                                                                        "time_in_seconds": 1,
                                                                        "geometry": 1})
cursor2.sort([("satellite_name", pymongo.ASCENDING), ("time_in_seconds", pymongo.ASCENDING)])
total_deleted_ships_number = 0
current_deletion_in_iteration = 0

keep_true = True
while keep_true:
    iterator = 0
    print(str(iterator) + ' iterations passed')
    for ship in cursor2:
        iterator += 1
        delete_copies_query = {"time_in_seconds": {'$lte': ship['time_in_seconds'] },
                               # "satellite_name": ship['satellite_name'],
                               "geometry": {"$geoIntersects": {"$geometry": ship['geometry']}}}
        current_deletion_in_iteration += ships_collection.delete_many(delete_copies_query).deleted_count - 1
        ships_collection.insert(ship)
        print(current_deletion_in_iteration)
    if current_deletion_in_iteration == 0:
        print('im here')
        keep_true = False
    else:
        total_deleted_ships_number += current_deletion_in_iteration
        current_deletion_in_iteration = 0
        cursor2 = ships_collection.find({"satellite_name": {'$exists': True}}, {"satellite_name": 1,
                                                                                "time_in_seconds": 1,
                                                                                "geometry": 1})
        cursor2.sort([("satellite_name", pymongo.ASCENDING), ("time_in_seconds", pymongo.ASCENDING)])


print(total_deleted_ships_number)
# #plotting histogram of ship scores
# # scores_array = turn_property_to_array("properties.score", ships_collection)
# # plt.hist(scores_array, bins=20)
# # plt.title("score distribution")
# # plt.gca().invert_xaxis()
# # plt.show()

# client.drop_database('working_db')
# fig = plt.gcf()
#
# plot_url = py.plot_mpl(fig, filename="score distribution hist")

#print(scores_array)




