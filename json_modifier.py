import json
import os.path
from datetime import datetime

input_file_path = 'data/json1.json'
output_file_path = 'data/json1_output.json'
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
        add_key_to_element(element, key, value)  # Add the key in it at immediate children level

        for sub_element in element.items():  # Iterate for all the children of this element
            if isinstance(sub_element[1], dict):  # If this child is also a dictionary
                add_key_value_recurssively(sub_element[1], key,
                                           value)  # call the same method recursively for this child
            elif isinstance(sub_element[1], list):  # If this child is an array
                for sub_item in sub_element[1]:  # Iterate each element of this child array
                    add_key_value_recurssively(sub_item, key, value)  # call the same method recursively for this child


def main(input_json_file_path, output_json_file_path):
    """
    This method Adds three keys namely "pkg_id", "ingestSourceFileName" and "..TransactionId" to the given json file

    The "pkg_id" is added at each message object inside the TransactionMessage array,
    example PolicyTransactionMessage[] and PolicyStatusMessage[] are two such array objects

    The "ingestSourceFileName" and "..TransactionId" is added to every element and sub-element of
    each TransactionMessage object

    It stores the modified json file at the given file path. It also make sure the size of output file is
    reduced to minimum required size, to achive that it removes all the unnecessary characters from the json,
    example white spaces between keys, tabs, new line characters, etc.


    :param input_json_file_path: The file path of input json to modify by adding three keys
    :param output_json_file_path: The file path of output json, where the modified json will be stored
    :return: None
    """
    with open(input_json_file_path) as f:
        json_data = json.load(f)

        # fetch the pkg_id to start with
        pkg_id = get_next_pkg_id()

        ingest_source_file_name_key = "ingestSourceFileName"
        for key, value in json_data.items():
            # As the name of TransactionMessage array is dynamic, so it tries to find it by checking its type
            if isinstance(value, list):  # Example: value= PolicyTransactionMessage:[]
                message_key = list(value[0].keys())[0]  # Example: message_key="PolicyTransactionMessage"

                # This is the actual loop which iterate through all the TransactionMessage one by one
                for message in value:  # Example: message=PolicyTransactionMessage:{}
                    message_value = message.get(message_key)

                    # Add the pkg_id to the TransactionMessage (example: PolicyTransactionMessage)
                    add_key_to_element(message_value, pkg_id_element_name, pkg_id)
                    pkg_id += 1  # increment it by one for next message

                    # Find out the ingestSourceFileName for this message,
                    # Note: It might be different for different messages
                    ingest_source_file_name_value = message_value.get(ingest_source_file_name_key)

                    # First Find out which is the Transaction Element,
                    # example: PolicyStatusTransaction, PolicyTransaction are two such examples
                    transaction_element_key = get_transaction_element_key(message_value)

                    # Find out the the TransactionId Element,
                    # example: ns0107_TransactionId, ns0478_TransactionID are two such examples
                    transaction_id_element = get_transaction_id_element(message_value[transaction_element_key])
                    transaction_id_element_key = transaction_id_element[0]
                    transaction_id = transaction_id_element[1]

                    # Add the "..TransactionId" recursively in each element and sub-element of the current message
                    add_key_value_recurssively(message_value, transaction_id_element_key, transaction_id)

                    # Add the "ingestSourceFileName" recursively in each element and sub-element of the current message
                    add_key_value_recurssively(message_value, ingest_source_file_name_key, ingest_source_file_name_value)

        # print(json.dumps(json_data, indent=2, sort_keys=True))

        # Save the modified json at given output file path
        with open(output_json_file_path, 'w', encoding='utf-8') as f2:
            json.dump(json_data, f2, ensure_ascii=False)

        # Store the next pkg_id on file system (__temp__.json) for next use
        persist_next_pkg_id(pkg_id)


started_on = datetime.utcnow()
main(input_file_path, output_file_path)
ended_on = datetime.utcnow()

print(f"{'Start Time'.ljust(25)}: {started_on.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
print(f'{"Input File Size (MB)".ljust(25)}: {"{:.2f}".format(os.stat(input_file_path).st_size / (1024 * 1024))}')
print(f'{"Output File Size (MB)".ljust(25)}: {"{:.2f}".format(os.stat(output_file_path).st_size / (1024 * 1024))}')
print(f"{'End Time'.ljust(25)}: {ended_on.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
