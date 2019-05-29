
from datetime import datetime as dt

def state_value(str):
    """
    :param str: Input Parameter is a Row from Map Function
    :return: Key Value pair, key is a Customer Id and value will be Customer Transaction id, date and amount separated
    by $, and each value will be separated by @
    """
    list_str = str.split(",")
    key = list_str[0]
    value = list_str[1]+"$"+list_str[2]+"$"+list_str[3]+"@"
    return (key, value)


def get_all_states(input_key, input_str):
    """
    :param input_key: Key as Customer Id
    :param input_str: Its List of Customer transactions values
    :return: Key value pair, Key as a Customer Id and Value will be List of States from Transactions.
    """
    result = []

    list_str = input_str.split("@")

    for id in range(0,len(list_str)):
        freq = ''
        if id+1 < len(list_str) :
            if len(list_str[id + 1].split("$")) > 1 :
                present_dt = list_str[id].split("$")[1]
                next_dt = list_str[id + 1].split("$")[1]

                """
                current and previous dates to compare the dates and find customer state on that particular transaction. 
                """
                p_d = dt.strptime(present_dt, "%d/%m/%y %H:%M")
                n_d = dt.strptime(next_dt, "%d/%m/%y %H:%M")
                d = n_d - p_d
                res_d = abs(d.days)
                if res_d >= 60:
                    freq += 'L'
                elif 30 < res_d < 60:
                    freq += 'M'
                elif res_d <= 30:
                    freq += 'S'

            if len(list_str[id + 1].split("$")) >= 2 :

                """
                Current and Prior amounts to calcute difference and their transaction state 
                """
                pres_amt = list_str[id].split("$")[2]
                next_amt = list_str[id + 1].split("$")[2]

                if float(pres_amt) < 1.1 * float(next_amt):
                    freq += 'E'
                elif float(pres_amt) < 0.9 * float(next_amt):
                    freq += 'L'
                else:
                    freq += 'G'
        if freq != '':
            result.append(freq)
    return (input_key, result)