

def probability(states_list):
    """
    :param states_list: Its a list of Customer States.
    :return: Customer states probability as a Dictionary.
    """
    rest = []
    tuples_list = []

    for idx in range(0,len(states_list)):
        if idx + 1 < len(states_list):
            tuples_list.append((states_list[idx],states_list[idx+1]))

    unique = set(tuples_list)

    for value in unique:
        rest.append((value, tuples_list.count(value)))
    return rest

