import mysql.connector
from geopy import Nominatim
from mysql.connector import ProgrammingError

locator = Nominatim(user_agent="myGeocoder", timeout=10)


def text_address_to_lat_lon(text_address):
    location = locator.geocode(text_address)
    if location:
        return location.latitude, location.longitude
    else:
        return None


if __name__ == '__main__':
    connection = mysql.connector.connect(host='localhost', database='dataengineer', user='ikram', password='ikram',
                                         ssl_disabled=True)

    sql_select_Query = "select * from address"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()

    ids_to_text_address = {}
    for row in records:
        ids_to_text_address[row[0]] = row[1] + ", " + row[3] + ", " + row[2]

    print(ids_to_text_address)
    print(len(ids_to_text_address))

    ids_to_latitude_longitude = {}
    for id, text_address in ids_to_text_address.items():
        print(id)
        ids_to_latitude_longitude[id] = text_address_to_lat_lon(text_address)

    print(ids_to_latitude_longitude)

    add_lat_lon_columns_query = "ALTER TABLE address ADD latitude DOUBLE DEFAULT NULL, ADD longitude DOUBLE DEFAULT NULL"
    try:
        cursor.execute(add_lat_lon_columns_query)
    except ProgrammingError as e:
        print(e)

    for id, latitude_longitude in ids_to_latitude_longitude.items():
        if latitude_longitude:
            latitude, longitude = latitude_longitude
            update_query = f"UPDATE address SET latitude = {latitude}, longitude = {longitude} WHERE address_id = {id};"
            cursor.execute(update_query)
    connection.commit()
    
    question_4_query= """
    select customer.first_name as 'Nom du client', customer.last_name as 'Prénom' , count(rental.rental_id) as 'Nombre de locations', CONCAT(address.address, ', ', address.postal_code, ', ', address.city) as 'Adresse postale', address.latitude, address.longitude
    from rental
    inner join customer on rental.customer_id = customer.customer_id
    inner join address on customer.address_id = address.address_id
    GROUP BY rental.customer_id
    ORDER BY 'Nombre de locations' DESC 
    LIMIT 1;
    """
    if connection.is_connected():
        connection.close()
        cursor.close()
        print("MySQL connection is closed")
