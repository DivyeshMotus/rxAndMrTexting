import pandas as pd

def getAllPatientsToText(dbCursor):
    tableQuery = '''
        SELECT
            s.story_id,
            c.contact_id,
            s.age_id,
            ins.created_at,
            s.status,
            i.product,
            c.first_name,
            c.phone_number
        FROM story_fresh AS s
        LEFT JOIN contacts_fresh AS c
        ON c.contact_id = s.destination
        LEFT JOIN insurance_fresh AS i
        ON i.insurance_id = s.story_id
        left join (
        select distinct on (insurance_id)
        	insurance_id, created_at
    		from insurance
    		order by insurance_id, created_at asc
		) ins
			on ins.insurance_id = s.story_id
        WHERE s.type = 'insurance'
        AND s.status IN (
            'needPrescriptionOnly',
            'needPrescriptionAndMedicalRecords',
            'needMedicalRecordsOnly'
            );
    '''
    dbCursor.execute(tableQuery)
    columnNames = [desc[0] for desc in dbCursor.description]
    rows = dbCursor.fetchall()
    df = pd.DataFrame(rows, columns=columnNames)
    return df

def getStatusBeginTime(ageId, dbCursor):
    tableQuery = '''
        SELECT
            a.begin_time
        FROM age_table AS a
        WHERE a.age_id = %s
    '''
    dbCursor.execute(tableQuery, (ageId, ))
    beginTime = dbCursor.fetchall()
    return beginTime[0][0]

def hasPatientOptedOut(patientContactId, dbCursor):
    tableQuery = '''
        SELECT
            s.status
        FROM story_fresh AS s
        WHERE s.destination = %s
            AND s.type = 'insuranceTextingOptout'
    '''
    dbCursor.execute(tableQuery, (patientContactId, ))
    status = dbCursor.fetchall()
    if len(status) == 0:
        return None
    else:
        return status[0][0]

def getTemplateFromDb(templateName, dbCursor):
    tableQuery = '''
        SELECT
            scc.text_message
        FROM status_change_communication AS scc
        WHERE scc.status = %s 
            AND scc.story_type = 'insurance'
    '''
    dbCursor.execute(tableQuery, (templateName, ))
    template = dbCursor.fetchall()
    if len(template) == 0:
        return None
    else:
        return template[0][0]


