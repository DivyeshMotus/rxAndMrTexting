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

def convertToUtc(ts):
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    t = pd.Timestamp(ts)
    if t.tzinfo is None or t.tz is None:
        return t.tz_localize('UTC')
    return t.tz_convert('UTC')

def calculatePatientDurationInPipeline(patient, dbCursor):
    durationInDays = -1
    if pd.isna(patient['age_id']):
        nowUtc = pd.Timestamp.now(tz='UTC')
        convertedCreatedAt = convertToUtc(patient['created_at'])
        differenceInTimes = nowUtc - convertedCreatedAt
        durationInDays = differenceInTimes.days
        if durationInDays == 0:
            return -1
    else:
        patientBeginTime = getStatusBeginTime(patient['age_id'], dbCursor)
        nowUtc = datetime.now(timezone.utc)
        differenceInTimes = nowUtc - patientBeginTime
        durationInDays = differenceInTimes.days
        if durationInDays == 0:
            return -1
    return durationInDays

def fillTemplate(template, patient):
    product = ''
    if patient['product'] == None:
        product = "Motus Hand or Foot"
    elif 'hand' in patient['product'].lower():
        product = "Motus Hand"
    else:
        product = "Motus Foot"
    
    return template.format(
        first_name=patient['first_name'],
        product=product
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
    messagesSent = 0
    readyMessages = pd.DataFrame()
    for index, patient in patientsToText.iterrows():
        if not (pd.isna(patient['first_name']) or pd.isna(patient['phone_number']) or pd.isna(patient['contact_id'])):
            optedOut = hasPatientOptedOut(patient['contact_id'], dbCursor)
            if optedOut == None:
                patientDuration = calculatePatientDurationInPipeline(patient, dbCursor)
                templateName = patient['status'] + '_' + str(patientDuration)
                template = getTemplateFromDb(templateName, dbCursor)
                if template != None:
                    filledTemplate = fillTemplate(template, patient)
                    # patient_dict = {'Contact_Id': patient['contact_id'], 'Template Name': templateName, 'Text Message': filledTemplate}
                    # readyMessages = pd.concat([readyMessages, pd.DataFrame([patient_dict])], ignore_index = True)
                    print(f"Insurance ID: {patient['story_id']}; Contact ID: {patient['contact_id']}; Template: {templateName}")
                    # sendMessage(client, filledTemplate, patient['phone_number'])
                    messagesSent += 1
                else:
                    continue
            else:
                continue
        else:
            continue
    sendMessage(client, f"Sent {messagesSent} RX and MR Messages today", "4704495817")
    dbCursor.close()
    dbConnection.close()


if __name__ == "__main__":
    main()