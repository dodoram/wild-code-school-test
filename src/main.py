import mysql.connector
from geopy import Nominatim
from mysql.connector import ProgrammingError

locator = Nominatim(user_agent="myGeocoder", timeout=10)
connection = mysql.connector.connect(host='localhost',
                                     database='dataengineer',
                                     user='ikram',
                                     password='Ikram1990!',
                                     ssl_disabled=True)
cursor = connection.cursor()


# Fonction qui récupère l'adresse pour chaque id, les mettre dans un dictionnaire id_adresse : addresse_postale
def get_ids_addresses():
    cursor.execute("select * from address")
    records = cursor.fetchall()
    ids_to_text_address = {}
    for row in records:
        ids_to_text_address[row[0]] = row[1] + ", " + row[3] + ", " + row[2]
    return ids_to_text_address


#Transfromation de l'adresse postale en adresse géographique
def convert_postal_addresses_to_lat_lon(ids_to_text_address):
    ids_to_latitude_longitude = {}
    for id, text_address in ids_to_text_address.items():
        ids_to_latitude_longitude[id] = text_address_to_lat_lon(text_address)
    return ids_to_latitude_longitude

# Fonction qui transforme l'adresse postale en coordonnées géographiques
def text_address_to_lat_lon(text_address):
    location = locator.geocode(text_address)
    if location:
        return location.latitude, location.longitude
    else:
        return None


# Ajout des colonnes latitude et longitude dans la table address
def add_lat_lon_columns():
    add_lat_lon_columns_query = "ALTER TABLE address ADD latitude DOUBLE DEFAULT NULL, ADD longitude DOUBLE DEFAULT NULL"
    try:
        cursor.execute(add_lat_lon_columns_query)
    except ProgrammingError as e:  # Pour ignorer l'erreur généré par la requête qui crée la colonne latitude et longitude
        print(e)


# Mise à jour de la table address en insérant les addresses géographiques dans les colonnes latitude, longitude
def insert_lat_lon_in_address_table(ids_to_latitude_longitude):
    for id, latitude_longitude in ids_to_latitude_longitude.items():
        if latitude_longitude:
            latitude, longitude = latitude_longitude
            update_query = f"UPDATE address SET latitude = {latitude}, longitude = {longitude} WHERE address_id = {id};"
            cursor.execute(update_query)
    connection.commit()


#Trouver le client fidèle
def print_most_loyal_client():
    client_fidele_query = """
    select customer.first_name as 'Nom du client', customer.last_name as 'Prénom' , count(rental.rental_id) as 'Nombre de locations', CONCAT(address.address, ', ', address.postal_code, ', ', address.city) as 'Adresse postale', address.latitude, address.longitude
    from rental
    inner join customer on rental.customer_id = customer.customer_id
    inner join address on customer.address_id = address.address_id
    GROUP BY rental.customer_id
    ORDER BY count(rental.rental_id) DESC
    limit 1;
    """
    cursor.execute(client_fidele_query)
    records = cursor.fetchone()
    print(records)


if __name__ == '__main__':
    # Récupération de l'adresse pour  chaque id
    ids_to_text_address = get_ids_addresses()
    print(ids_to_text_address)

    # Transfromation de l'adresse postale en adresse géographique
    ids_to_latitude_longitude = convert_postal_addresses_to_lat_lon(ids_to_text_address)
    print(ids_to_latitude_longitude)

    # Ajout des colonnes latitude et longitude dans la table address
    add_lat_lon_columns()

    # Insertion des données dans les colonnes latitude, longitude
    insert_lat_lon_in_address_table(ids_to_latitude_longitude)

    # Query_qst4 : trouver le client fidèle
    print_most_loyal_client()

    if connection.is_connected():
        connection.close()
        cursor.close()
        print("MySQL connection is closed")
