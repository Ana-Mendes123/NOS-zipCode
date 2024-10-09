import os
import typing
import requests
from requests import Response
import csv
import pandas as pd
import time
import mysql.connector as mc


def main():
	with open('csv/codigos_postais.csv', newline='') as csvfile:
		reader = csv.reader(csvfile, delimiter=',')
		for row in reader:
			zipcode = row[0]
			if row[0] != 'zipcode':
				print(row)
				time.sleep(2)
				json_list = zipToLocation(zipcode)
				if json_list == []:
					write_to_csv({}, zipcode)
					fields = {'concelho': '',
					          'distrito': ''}
					store_in_db(zipcode, fields)
				else:
					fields = parseJson(json_list)
					write_to_csv(fields, zipcode)
					store_in_db(zipcode, fields)


def zipToLocation(zipcode):
	url = 'https://www.cttcodigopostal.pt/api/v1'
	api_key = '0dde8fb14fc541dba99bb24fc0ddb039'
	cp4, cp3 = map(str, zipcode.split('-'))  #mapear os resulados do split para cp4 e cp3, resp

	#cp4 = '7860'
	#cp3 = '007'

	full_url = f'{url}/{api_key}/{cp4}-{cp3}'

	request: Response = requests.get(full_url)

	print(request.status_code)
	print(request.json())

	json_list = request.json()
	#json_dict = {'morada': 'Largo de São Francisco ', 'porta': '', 'localidade': 'Moura', 'freguesia': 'União das freguesias de Moura (Santo Agostinho e São João Baptista) e Santo Amador', 'concelho': 'Moura', 'distrito': 'Beja', 'latitude': '38.1386028', 'longitude': '-7.4508758', 'codigo-postal': '7860-007', 'info-local': '', 'codigo-arteria': '351002', 'concelho-codigo': 10, 'distrito-codigo': 2}

	return json_list


def parseJson(json_list):
	fields = {}
	for item in json_list:
		fields = {'concelho': item['concelho'],
		          'distrito': item['distrito']}
		print(fields)
		break
	return fields


def write_to_csv(fields: dict, zipcode: str):
	df = pd.DataFrame()
	if fields == {}:
		new_dict = {'zipcode': zipcode, 'concelho': '', 'distrito': ''}
	else:
		new_dict = {'zipcode': zipcode,
		            'concelho': fields['concelho'],
		            'distrito': fields['distrito']}
		print(new_dict)

	new_row_df = pd.DataFrame([new_dict])  #tranformar lista do dicionário em dataframe
	print(new_row_df)

	df = pd.concat([df, new_row_df], ignore_index=True)  #concatenar dataframe vazio com dataframe que quero

	# Write the DataFrame back to the CSV file
	file_exists = os.path.exists("csv/codigos_postais_novo.csv")

	df.to_csv("csv/codigos_postais_novo.csv", index=False, mode='a', header=not file_exists)


def store_in_db(zipcode: str, fields: dict):
	connection = mc.connect(
		host='localhost',
		database='NOS_ZIPCODE',
		user='anaisa',
		password='12345'
	)
	cursor = connection.cursor()
	add_record = ("INSERT INTO NOS_ZIPCODE.NOS_ZIPCODE "
	              "(zipcode, county, district)"
	              "VALUES (%s, %s, %s)")
	data_record = (zipcode, fields['concelho'], fields['distrito'])

	cursor.execute(add_record, data_record)
	connection.commit()

	print(f"Inserted {cursor.rowcount} row(s) into the table")


if __name__ == '__main__':
	main()
