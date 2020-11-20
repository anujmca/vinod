import json
import os.path
from datetime import datetime

input_file_path = 'data/json5.json'
output_file_path = 'data/json5_output.json'
pkg_id_element_name = "pkg_id"
temp_file_path = 'data/__temp__.json'


def get_next_pkg_id():
    """
    This method is used to get the next value in sequence for pkg_id.
    If the last persisted value of next pkg_id is not found on file system, then it returns 1 as the starting value

    Note: It tries to find a temporary json file with name as __temp__.json (this name is configurable)

    :return: Returns a int type value, which represents the next value available in the sequence for pkg_id key
    """
    first_pkg_id = 1
    next_id = int(first_pkg_id)
    try:
        # if the temp file __temp__json exists then read the
        # next value of pkg_id from it, else start from 1
        if os.path.isfile(temp_file_path):
            with open(temp_file_path) as f:
                temp_data = json.load(f)
                next_id = int(temp_data[pkg_id_element_name])

    except:
        if not isinstance(next_id, int):
            next_id = first_pkg_id

    return next_id


def persist_next_pkg_id(next_pkg_id):
    """
    This method stores the value of pkg_id to the temp file __temp__.json (this name is configurable).
    The value stored in this file will be used while executing the next file by this json modifier
    :param next_pkg_id: value to store
    :return: None
    """
    temp_data = {pkg_id_element_name: next_pkg_id}

    with open(temp_file_path, 'w', encoding='utf-8') as f:
        json.dump(temp_data, f, ensure_ascii=False, indent=2)


def add_key_to_element(element, key, value):
    """
    This method simply adds a key and it's value to the an element.
    Note: In case the key is already existing in the given element, then it doesn't change it
    :param element: The dictionary object where the key will be added
    :param key: The key to be added
    :param value: The value to be assigned
    :return: None
    """
    if isinstance(element, dict):
        if key not in element.keys():
            element[key] = value


def get_transaction_element_key(message):
    """
    This method scans the given json/dictionary and finds the element with name ending with "Transaction"
    (case insensitive match)

    This is required, because the element which contains the "TransactionId" is different in different files,
    example PolicyTransaction and PolicyStatusTransaction are two such example of different element name.

    :param message: dictionary object to scan for element ending with "Transaction"
    :return: str type value, example "PolicyTransaction", representing name of Transaction element
    """
    transaction_element_key = {key for key in message.keys() if key.lower().endswith('transaction')}
    return None if not bool(transaction_element_key) else transaction_element_key.pop()


def get_transaction_id_element(message):
    """
    This method scans the given dictionary and finds an element with name ending with "TransactionId" (case insensitive match)
    Once found, it returns the value this element holds.

    This is required, because the element containing value of "TransactionId" is different in all the messages,
    example ns0107_TransactionId, ns0478_TransactionID are two such examples of different element name

    :param message: the json object for "Transaction" element, which is found earlier using "get_transaction_element_key" methos
    :return: str type value, example "aa342cfe_938b_4b6f_911c_5f31a551bb59" representing TransactionId
    """
    transaction_id_element = {(key, value) for key, value in message.items() if key.lower().endswith('transactionid')}
    return None if not bool(transaction_id_element) else transaction_id_element.pop()


def add_key_value_recurssively(element, key, value):
    """
    This method adds the key value to an element and all its sub-elements till infinity depth and till infinity width,
    that is to all it's children and to all siblings of each children and then children of children.

    This works using recursion logic to achieve this dynamism
    :param element: The dictionary object where the key will be added along with all its children, grand children and so on
    :param key: The key to be added
    :param value: The value to be added
    :return: None
    """
    # Check if the given element is a json/dictionary object
    if isinstance(element, dict):
        add_key_to_element(element, key, value) # Add the key in it at immediate children level

        for sub_element in element.items():  # Iterate for all the children of this element
            if isinstance(sub_element[1], dict):  # If this child is also a dictionary
                add_key_value_recurssively(sub_element[1], key, value)  # call the same method recursively for this child
            elif isinstance(sub_element[1], list):  # If this child is an array
                for sub_item in sub_element[1]:  # Iterate each element of this child array
                    add_key_value_recurssively(sub_item, key, value)  # call the same method recursively for this child


print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

with open(input_file_path) as f:
    json_data = json.load(f)

    pkg_id = get_next_pkg_id()

    for key, value in json_data.items():
        if isinstance(value, list):  # Example: value= PolicyTransactionMessage:[]
            message_key = list(value[0].keys())[0]  # Example: message_key="PolicyTransactionMessage"

            for message in value:  # Example: message=PolicyTransactionMessage:{}
                message_value = message.get(message_key)

                add_key_to_element(message_value, pkg_id_element_name, pkg_id)
                pkg_id += 1

                ingestSourceFileName = message_value.get('ingestSourceFileName')
                transaction_element_key = get_transaction_element_key(message_value)
                transaction_id_element = get_transaction_id_element(message_value[transaction_element_key])
                transaction_id_element_key = transaction_id_element[0]
                transaction_id = transaction_id_element[1]

                add_key_value_recurssively(message_value, transaction_id_element_key, transaction_id)
                add_key_value_recurssively(message_value, 'ingestSourceFileName', ingestSourceFileName)

    # print(json.dumps(json_data, indent=2, sort_keys=True))

    with open(output_file_path, 'w', encoding='utf-8') as f2:
        json.dump(json_data, f2, ensure_ascii=False)

    persist_next_pkg_id(pkg_id)
print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
