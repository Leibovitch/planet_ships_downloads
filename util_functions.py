def parse_property_key(property_key):
    newsted_key_array = property_key.split('.')
    full_string = []
    for word in newsted_key_array:
        full_string.append('["' + word + '"]')

    full_string = ''.join(full_string)
    return full_string


def turn_property_to_array(property_key, mongo_collection):
    cursor = mongo_collection.find({}, {property_key: 1})
    new_prorperty_array = []
    full_string = parse_property_key(property_key)
    for element in cursor:
        exec('new_prorperty_array.append(element' + full_string + ')')

    return new_prorperty_array




