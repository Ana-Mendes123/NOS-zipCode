import os
import requests
from requests import Response
import csv
import pandas as pd
import time
import mysql.connector as mc
import typing

nulls = 0
completed = 0

def main():
	verification = verify_csv_exists()
	file_name = verification[0]
	exists = verification[1]
	if exists:
		if os.path.getsize(f"csv/{file_name}") == 0:
			print("File is empty")
			return
		df = pd.read_csv(f"csv/{file_name}")
		lines = df.shape[0] #shape = (nº de linhas, nº de colunas)
		current_line_number = 2

		for row in df.itertuples(index=False):
			print(f"Current line: {current_line_number}")
			if lines > 10:
				zipcode = row[0]
				time.sleep(2)
				print(row) #para ele não pôr logo os dados da próxima linha
				json_list = zipToLocation(zipcode)
				if json_list != []:
					fields = parseJson(json_list)
					write_to_csv(fields, zipcode)
					store_in_db(zipcode, fields)
				lines -= 1
				current_line_number += 1
				print(f"There are {lines} lines left")
				print("-------------------------------------------")
			else:
				print(f"There were {nulls} nulls and {completed} completed")
				return
	else:
		print("File not found.")
		return
	print(f"There were {nulls} nulls and {completed} completed")

def verify_csv_exists() -> [str,bool]:
	file_name = input("Enter csv file name: ")
	if not os.path.exists(f"csv/{file_name}"):
		return ["",False]
	else:
		return [file_name,True]



def zipToLocation(zipcode):
	url = 'https://www.cttcodigopostal.pt/api/v1'
	api_key = '0dde8fb14fc541dba99bb24fc0ddb039'
	cp4, cp3 = map(str, zipcode.split('-'))  #mapear os resulados do split para cp4 e cp3, resp

	#cp4 = '7860'
	#cp3 = '007'

	full_url = f'{url}/{api_key}/{cp4}-{cp3}'

	request: Response = requests.get(full_url)

	#print(request.status_code)
	#print(request.json())

	json_list = request.json()
	#json_dict = {'morada': 'Largo de São Francisco ', 'porta': '', 'localidade': 'Moura', 'freguesia': 'União das freguesias de Moura (Santo Agostinho e São João Baptista) e Santo Amador', 'concelho': 'Moura', 'distrito': 'Beja', 'latitude': '38.1386028', 'longitude': '-7.4508758', 'codigo-postal': '7860-007', 'info-local': '', 'codigo-arteria': '351002', 'concelho-codigo': 10, 'distrito-codigo': 2}

	global nulls, completed
	if json_list == []:
		nulls += 1
		print(f"nulls: {nulls}, completed: {completed}")
	else:
		completed += 1
		print(f"nulls: {nulls}, completed: {completed}")
	return json_list


def parseJson(json_list):
	for item in json_list:
		fields = {'concelho': item['concelho'],
		          'distrito': item['distrito']}
		#print(fields)
		return fields


def write_to_csv(fields: dict, zipcode: str):
	df = pd.DataFrame()
	new_dict = {'zipcode': zipcode,
	            'concelho': fields['concelho'],
	            'distrito': fields['distrito']}
	print(new_dict)

	new_row_df = pd.DataFrame([new_dict])  #tranformar lista do dicionário em dataframe
	#print(new_row_df)

	df = pd.concat([df, new_row_df], ignore_index=True)  #concatenar dataframe vazio com dataframe que quero

	# Write the DataFrame back to the CSV file
	file_exists = os.path.exists("outputs/codigos_postais_novo.csv")

	df.to_csv("outputs/codigos_postais_novo.csv", index=False, mode='a', header=not file_exists)


def store_in_db(zipcode: str, fields: dict):
	connection = mc.connect(
		host='localhost',
		database='NOS_ZIPCODE_SCHEMA',
		user='anaisa',
		password='12345'
	)
	cursor = connection.cursor()

	if record_exists(zipcode, cursor):
		return

	add_record = ("INSERT INTO NOS_ZIPCODE_SCHEMA.NOS_ZIPCODE "
	              "(zipcode, county, district)"
	              "VALUES (%s, %s, %s)")
	data_record = (zipcode, fields['concelho'], fields['distrito'])

	cursor.execute(add_record, data_record)
	connection.commit()

	print(f"Inserted {cursor.rowcount} row(s) into the table")

def record_exists(zipcode, cursor):
	query = (f"SELECT zipcode "
	         "FROM NOS_ZIPCODE_SCHEMA.NOS_ZIPCODE "
	         "WHERE zipcode = %s")

	# deve-se passar os parâmetros como tuplo, mesmo se for só 1
	cursor.execute(query, (zipcode,))
	result = cursor.fetchone()

	if result is not None: #verifica se result contém um valor
		print(f"Zipcode {result[0]} already exists in the database")
		return True
	else:
		return False

if __name__ == '__main__':
	main()
