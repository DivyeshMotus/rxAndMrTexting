from db_tools import *
from config import *
import dotenv
import os
from twilio.rest import Client
import psycopg2
import pandas as pd
from datetime import datetime, timezone

dotenv.load_dotenv()
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')
DIVYESH_PHONE = os.getenv('DIVYESH_PHONE')

def createConnection():
    params = game_db_config()
    gameDbConn = psycopg2.connect(**params)
    gameDbCur = gameDbConn.cursor()
    return gameDbConn, gameDbCur

def calculatePatientDurationInPipeline(patient, dbCursor):
    durationInDays = -1
    if pd.isna(patient['age_id']):
        durationInDays = 15
    else:
        patientBeginTime = getStatusBeginTime(patient['age_id'], dbCursor)
        # print(f"Story_id: {patient['story_id']}, Being Time: {patientBeginTime}")
        now_utc = datetime.now(timezone.utc)
        differenceInTimes = now_utc - patientBeginTime
        durationInDays = differenceInTimes.days
        if durationInDays == 0:
            return -1
    return durationInDays

def fillTemplate(template, patient):
    return template.format(
        first_name=patient['first_name'],
        product=patient['product']
    )

def sendMessage(client, filledTemplate, phoneNumber):    
    messageBody = f"{filledTemplate}"
    try:
        message = client.messages.create(
            body=messageBody, 
            from_=TWILIO_PHONE_NUMBER,
            to=phoneNumber
        )
    except Exception as e:
        print(f"Failed to send SMS to Divyesh: {e}")

def main():
    dbConnection, dbCursor = createConnection()
    patientsToText = getAllPatientsToText(dbCursor)
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    for index, patient in patientsToText.iterrows():
        if not (pd.isna(patient['first_name']) or pd.isna(patient['phone_number']) or pd.isna(patient['contact_id'])):
            optedOut = hasPatientOptedOut(patient['contact_id'], dbCursor)
            if optedOut == None:
                patientDuration = calculatePatientDurationInPipeline(patient, dbCursor)
                templateName = patient['status'] + '_' + str(patientDuration)
                template = getTemplateFromDb(templateName, dbCursor)
                if template != None:
                    filledTemplate = fillTemplate(template, patient)
                    sendMessage(client, filledTemplate, patient['phone_number'])
                else:
                    continue
            else:
                continue
        else:
            continue
        
    dbCursor.close()
    dbConnection.close()


if __name__ == "__main__":
    main()