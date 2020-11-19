

# Parse one row from json file creating:
# 1. A string of keys AKA columns in the table
# 2. A list of values to be inserted, Respectively.
def parse_row(row):
    key_list = "("
    value_list = []
    first_pair = True
    for key, value in row.items():
        if not first_pair:
            key_list += ", "
        first_pair = False
        key_list += key
        value_list.append(value)
    key_list += ")"
    return key_list, tuple(value_list)
