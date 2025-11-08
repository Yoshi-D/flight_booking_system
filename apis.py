from fastapi import FastAPI, Request
from generate_ids import *
from db import *

app = FastAPI()
#made functionality to
# insert new passenger,
# get specific flight details ,
# generate and insert a new ticket for a particular passenger,
# get all tickets (with destination and source airports) for a certain passenger

#next steps
#display available seats for a particular schedule_id flight
#cancel ticket, update ticket (seat wise) ,get ticket by pnr

@app.get("/show_tables")
def show_db_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("show tables;")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows


@app.post("/insert_into_passenger")
async def insert_into_passenger_table(request: Request):

    data = await request.json()
    passenger_id = generate_passenger_id() # this is how our primary key is decided

    gov_id_type = data.get("gov_id_type")
    doc_id_number = data.get("doc_id_number")
    name = data.get("name")
    dob = data.get("DOB")
    gender = data.get("gender")
    nationality = data.get("nationality")
    email = data.get("email")

    conn = get_connection()
    cursor = conn.cursor()

    query = """
            INSERT INTO passenger
            (passenger_id, gov_id_type, doc_id_number, name, DOB, gender, nationality, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    cursor.execute(query, (
        passenger_id, gov_id_type, doc_id_number, name,
        dob, gender, nationality, email
    ))

    conn.commit()
    cursor.close()
    conn.close()
    return {"status": "200", "passenger_id": passenger_id}

@app.get("/get_flight_details") #this takes in dates as well as airports
async def get_flight_details(from_date, to_date, source_airport, destination_airport):
    #from date, to date, from which airport, to which airport

    conn = get_connection()
    cursor = conn.cursor()

    query = """
                select * from routes natural join flight_schedule
                where flight_schedule.schedule_date between %s and %s and routes.start_ap = %s and routes.dest_ap = %s
            """
    cursor.execute(query, (
        from_date, to_date, source_airport, destination_airport
    ))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    flights_data = []
    if len(rows)>0:
        flight = {}
        for row in rows:
            flight["route_id"] = row[0]
            flight["start_ap"] = row[1]
            flight["dest_ap"] = row[2]
            flight["distance"] = row[3]
            flight["schedule_id"] = row[4]
            flight["flight_id"] = row[5]
            flight["schedule_data"] = row[6]
            flight["dep_time"] = row[7]
            flight["arrival_time"] = row[8]
            flights_data.append(flight)
    return flights_data

@app.post("/generate_ticket")
async def insert_into_booking_table(request: Request):
    data = await request.json()
    pnr = generate_pnr()
    booker_id = data.get("passenger_id")
    status_code = data.get("status_code")
    schedule_id = data.get("schedule_id")
    class_code = data.get("class_code")
    price = data.get("price")
    seat_number = data.get("seat_number")

    conn = get_connection()
    cursor = conn.cursor()
    query = """
            INSERT INTO booking (pnr, booker_id, status_code)
            VALUES (%s, %s, %s)
        """
    cursor.execute(query, (pnr, booker_id, status_code))

    ticket_id = generate_ticket_id()
    query = """
                INSERT INTO ticket (ticket_id, pnr, passenger_id, schedule_id, class_code, price, seat_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
    cursor.execute(query, (ticket_id,pnr, booker_id,schedule_id, class_code, price, seat_number))

    conn.commit()
    cursor.close()
    conn.close()

    return {"status": "200", "pnr":pnr, "ticket_id":ticket_id}


@app.get("/get_passenger_tickets")
async def get_passenger_ticket(passenger_id):

    conn = get_connection()
    cursor = conn.cursor()

    query = """
                    SELECT
                    t.pnr,
                    t.seat_number,
                    t.class_code,
                    t.price,
                    fs.schedule_date,
                    fs.dep_time,
                    fs.arrival_time,
                    r.start_ap,
                    r.dest_ap
                FROM passenger p
                JOIN ticket t ON t.passenger_id = p.passenger_id
                JOIN flight_schedule fs ON fs.schedule_id = t.schedule_id
                JOIN routes r ON r.route_id = fs.route_id
                WHERE p.passenger_id = %s;
                """
    cursor.execute(query, (passenger_id,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    ticket_data = []
    if len(rows)>0:
        for row in rows:
            ticket = {}
            ticket["pnr"] = row[0]
            ticket["seat_number"] = row[1]
            ticket["class_code"] = row[2]
            ticket["price"] = row[3]
            ticket["schedule_date"] = row[4]
            ticket["dep_time"] = row[5]
            ticket["arrival_time"] = row[6]
            ticket["start_ap"] = row[7]
            ticket["dest_ap"] = row[8]
            ticket_data.append(ticket)

    return ticket_data

@app.get("/check_user_exists")
async def check_user_exists(passenger_id):

    conn = get_connection()
    cursor = conn.cursor()

    query = """
                    select * from passenger where passenger_id = %s
            """
    cursor.execute(query, (passenger_id,))

    row = cursor.fetchone()
    cursor.close()
    conn.close()
    passenger_data = {}
    if row:
        passenger_data["passenger_id"] = row[0]
        passenger_data["gov_id_type"] = row[1]
        passenger_data["phone_number"] = row[2]
        passenger_data["name"] = row[3]
        passenger_data["dob"] = row[4]
        passenger_data["gender"] = row[5]
        passenger_data["nationality"] = row[6]
        passenger_data["email_address"] = row[7]

    return passenger_data

