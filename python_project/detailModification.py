import pandas as pd
import logging as log
log.basicConfig(level=log.INFO)
import mysql.connector
import datetime,os,threading
 
fileLocation='C:\\Users\\805831\\Desktop\\python_project\\importFile\\'
fileList = [fl for fl in os.listdir(fileLocation) if fl.endswith(".csv")]
print(fileList)

try:
    db = mysql.connector.connect(host="localhost", user="root", passwd="root", db="account_detail_schemas")
    cursor = db.cursor()
except  mysql.connector.Error as error:
    log.error("Something went wrong in SQL connection "+str(error))

def dataProcessing(data):
    sqlInsert = "INSERT INTO account_detail (ACCOUNT_ID,ACCOUNT_NAME) VALUES (%s,%s)"
    sqlUpdate = "UPDATE account_detail SET ACCOUNT_NAME = %s WHERE ACCOUNT_ID = %s"
    sqlDelete = "DELETE FROM account_detail WHERE ACCOUNT_ID= %s"
    if(data[2]=="I"):
        try:
            cursor.execute(sqlInsert,(data[0],data[1]))
            db.commit()
        except:
            log.error("Error in Inserting {}".format(data))
            # db.rollback()
    elif(data[2]=="U"):
        try:
            cursor.execute(sqlUpdate,(data[1],data[0]))
            db.commit()
        except:
            log.error("Error in Update {}".format(data))
            # db.rollback()        
    elif(data[2]=="D"):
        try:
            cursor.execute(sqlDelete,(data[0],))
            db.commit()
        except:
            log.error("Error in Delete {}".format(data))
            # db.rollback()   
    else:
        log.error("Unidentified mode for {}".format(data))
       

def csvTodb(file_loc,file_name):
    for iterator in range(0,len(file_name)):
        filepath = file_loc+file_name[iterator]
        print(file_name[iterator])
        try:
            df = pd.read_csv(filepath)
        except (FileNotFoundError,IOError) as fileError:
            log.error(fileError)
        else:
            for data in df.values.tolist():
                print(data)
                dataProcessing(data)  

div = len(fileList)/3
rem =len(fileList)%3
# print(type(fileList[0:int(div)]))
# firstThread = threading.Thread(target=csvTodb,args=(fileLocation,fileList[0:int(div)]))            
secondThread = threading.Thread(target=csvTodb,args=(fileLocation,fileList[int(div):int(div*2)]))
# thirdThread = threading.Thread(target=csvTodb,args=(fileLocation,fileList[int(div*2):int(div*3+rem+1)]))
# firstThread.start()
secondThread.start()
# thirdThread.start()
# firstThread.join()
secondThread.join()
# thirdThread.join()
# cursor.close()
# db.close()

'''
            if("I" in df['MODE'].tolist()):
                id = df[df['MODE']=="I"].ACCOUNT_ID.tolist()
                name = df[df['MODE']=="I"].ACCOUNT_NAME.tolist()
                print(type(id))
                print(x)
                print(y)
'''
# for item in range(fileList): 
# csvTodb('C:\\Users\\805831\\Desktop\\python_project\\importFile\\'+str(fileList[item]))            
#connection.close()
    
