from db import *

def generate_passenger_id():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        select passenger_id 
        from passenger
        order by cast(substring(passenger_id, 2) AS unsigned) desc
        limit 1
    """
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        last_id = result[0]
        last_num = int(last_id[1:])
        new_num = last_num + 1
    else:
        new_num = 1

    cursor.close()
    conn.close()
    return f"P{new_num}"

def generate_ticket_id():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        select ticket_id
        from ticket
        order by cast(substring(ticket_id, 2) AS unsigned) desc
        limit 1
    """
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        last_id = result[0]
        last_num = int(last_id[1:])
        new_num = last_num + 1
    else:
        new_num = 1

    cursor.close()
    conn.close()
    return f"T{new_num}"

def generate_pnr():
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        select pnr 
        from booking
        order by cast(substring(pnr, 4) AS unsigned) desc
        limit 1
    """
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        last_id = result[0]
        last_num = int(last_id[3:])
        new_num = last_num + 1
    else:
        new_num = 1

    cursor.close()
    conn.close()
    return f"PNR{new_num}"