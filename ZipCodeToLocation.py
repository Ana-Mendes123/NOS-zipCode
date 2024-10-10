import os
import requests
from requests import Response
import pandas as pd
import time
import mysql.connector as mc

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
		line = check_exit_file()
		df = pd.read_csv(f"csv/{file_name}", skiprows=[i for i in range(1, line-1)])
		lines = df.shape[0] #shape = (nº de linhas, nº de colunas)
		if line == -1:
			current_line_number = 2
		else:
			current_line_number = line
		try:
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
		except KeyboardInterrupt:
			with open("outputs/exit.txt", "w") as log_file:
				log_file.write(f"{current_line_number}")
				print(f"\nInterrupted at line {current_line_number}. Writing to log...")
				return
	else:
		print("File not found.")
		return
	print(f"There were {nulls} nulls and {completed} completed")
	with open("outputs/exit.txt", "w") as log_file:
		log_file.write("Terminated")

def check_exit_file():
	if os.path.exists("outputs/exit.txt"):
		with open("outputs/exit.txt", "r") as log_file:
			log_read = log_file.read()
			if log_read != "Terminated":
				line: int = int(log_read)
				return line
			else:
				return -1
	return -1

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

	full_url = f'{url}/{api_key}/{cp4}-{cp3}'

	request: Response = requests.get(full_url)

	#print(request.status_code)
	#print(request.json())

	json_list = request.json()

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
		fields = {'county': item['concelho'],
		          'district': item['distrito']}
		#print(fields)
		return fields


def write_to_csv(fields: dict, zipcode: str):
	df = pd.DataFrame()
	new_dict = {'zipcode': zipcode,
	            'county': fields['county'],
	            'district': fields['district']}
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
	data_record = (zipcode, fields['county'], fields['district'])

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
