'''
Created By : Jitendra Sharma
Created On : 15-Jul-2021
Description : To merge features of DMP incremental builds.
'''

''' Import libraries '''
import sys, os
import arcpy
from arcpy import env
from time import localtime, strftime
import getpass
import zipfile
import ntpath

###global variables#####
appVersion = "v1.0"
logFolderPath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "Logs")

#StateList = ['ZZ']
StateList = ['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','ZZ']

arcpy.env.overwriteOutput='True'

###################################################################################################################
'''logs log messages to text log file'''
def logMessage(logFilePath, msg):
    #with lock:
    if not os.path.exists(logFolderPath):
        os.makedirs(logFolderPath)
    if os.path.exists(logFilePath):
        openMode = 'a'
    else:
        openMode = 'w'
    fh = open(logFilePath, openMode)
    pid = os.getpid()
    timestamp = '['+strftime("%Y-%m-%d %H:%M:%S", localtime())+']  '+str(pid)
    timestamp = timestamp.ljust(32)
    msg = timestamp+ " "+msg
    if(openMode=='w'):
        fh.write(timestamp + " "+ "User: {0}".format(getpass.getuser()))
    fh.write("\n")
    fh.write(msg)
    fh.close()
    print(msg)

###################################################################################################################
'''Extracts zip(.zip) files if its not already extracted'''
def extractFilesIfNotExtracted(path):
    if (path.endswith(".zip")):
        extractedFilePath = path.split('.zip')[0]
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(extractedFilePath)
        #extractedFilePath = path.split('.zip')[0]
        #zipFilePath = path
        #if (not os.path.exists(extractedFilePath)):
        #    with gzip.open(zipFilePath, 'rb') as f_in:
        #        with open(extractedFilePath, 'wb') as f_out:
        #            shutil.copyfileobj(f_in, f_out)

###################################################################################################################
'''Checks and extracts zip(.gz) files if its not already extracted'''
def checkAndExtractFilesIfNotExtracted(path):
    if(os.path.isdir(path)):
        for file in os.listdir(path):
            #extractFilesIfNotExtracted(os.path.join(path, file))
            checkAndExtractFilesIfNotExtracted(os.path.join(path, file))
    elif(os.path.isfile(path)):
        logMessage(logFilePath, str(path))
        extractFilesIfNotExtracted(path)

###################################################################################################################
'''Loop through all the folders in the path and get available geodatabase files for all available states'''
def checkGdbfolders(path, gdbListIn, state = r''):
    gdbList = gdbListIn
    try:
        if (os.path.isdir(path)):
            if (state == ''):
                if (path.endswith('.gdb')):
                    #print(path)
                    gdbList.append(str(path))
                else:
                    for file in os.listdir(path):
                        gdbList = checkGdbfolders(os.path.join(path, file), gdbList, state)
            else:
                if (path.endswith('.gdb')):
                    #head, tail = ntpath.split(path)
                    gdbName = os.path.basename(path)
                    if (gdbName.startswith(state)):
                        #print(path)
                        gdbList.append(str(path))
                else:
                    for file in os.listdir(path):
                        gdbList = checkGdbfolders(os.path.join(path, file), gdbList, state)

        return gdbList
    except Exception as e:
        print("An error occurred. Error {0}".format(str(e)))
        logMessage(logFilePath, "Error: {}".format(str(e)))
        return []

###################################################################################################################
'''It copies the attributes of the fields that exist at the target shapefile'''
def merge(target, source):
    try:
        logMessage(logFilePath, "--------------------------------------------------------------------------------")
        logMessage(logFilePath, "Start merging geodatabase : {} into {}".format(source, target))
        env.workspace = source
        datasetList = arcpy.ListDatasets('*', 'FeatureDataset')

        for dataset in datasetList:
            env.workspace = source
            arcpy.env.workspace = dataset
            fcList = arcpy.ListFeatureClasses()

            if (fcList is not None):
                for fc in fcList:
                    if (fc is not None):
                        #print arcpy.env.workspace, fc
                        logMessage(logFilePath, "Process merging of feature : {}".format(fc))

                        out_featureclass = os.path.join(target, os.path.splitext(fc)[0])

                        if arcpy.Exists(out_featureclass):
                            arcpy.Delete_management(out_featureclass)

                        arcpy.Copy_management(fc, out_featureclass)
    except Exception as e:
        print("An error occurred. Error {0}".format(str(e)))
        logMessage(logFilePath, "Error: {}".format(str(e)))

#################### main method ###################################
if __name__ == "__main__":
    print("Started")
    appStartTimestamp = strftime("%Y_%m_%d_%H_%M_%S", localtime())
    logFilePath = os.path.join(logFolderPath, "Process_Log_" + appVersion + "_" + appStartTimestamp + ".txt")

    logMessage(logFilePath, "********************************************************************************")
    logMessage(logFilePath, "Start DMP geodatabase merging process.")

    #t_gdbPath = r"D:\SpatialData_2021\DMP_V21\AK.gdb"
    #s_gdbPath = r"D:\SpatialData_2021\DMP\DMP_DELIVERY_20201104\ZIPS\AK_20201104\AK_20201104.gdb"

    path = r"D:\Jitendra\Data\SpatialData_2021\DMP"

    logMessage(logFilePath, "Started extracting zipped files.")
    checkAndExtractFilesIfNotExtracted(path)
    logMessage(logFilePath, "Finished extracting zipped files.")

    for state in StateList:
        logMessage(logFilePath, "Starting merging process for state : '{}'.".format(state))
        gdbOnPath = []
        gdbOnPath = checkGdbfolders(path, gdbOnPath, state)

        t_gdbPath = r"D:\Jitendra\Data\SpatialData_2021\DMP_V21\{}.gdb".format(state)

        for gdb in gdbOnPath:
            print("Process merging for : " + gdb)
            s_gdbPath = gdb
            merge(t_gdbPath, s_gdbPath)

    #merge(t_gdbPath, s_gdbPath)

    #env.workspace = s_gdbPath
    #datasetList = arcpy.ListDatasets('*', 'FeatureDataset')

    #for dataset in datasetList:
    #    env.workspace = s_gdbPath
    #    arcpy.env.workspace = dataset
    #    fcList = arcpy.ListFeatureClasses()

    #    if (fcList is not None):
    #        for fc in fcList:
    #            if (fc is not None):
    #                print arcpy.env.workspace, fc

    #                out_featureclass = os.path.join(t_gdbPath, os.path.splitext(fc)[0])

    #                if arcpy.Exists(out_featureclass):
    #                    arcpy.Delete_management(out_featureclass)

    #                arcpy.Copy_management(fc, out_featureclass)


        #VINTAGE = '20201104'
        #STATECODE = 'AK'
        #COUNTYNAME = str(dataset)

        #input_path = r'D:\SpatialData_2021\DMP\DMP_DELIVERY_20201104\ZIPS\{}_{}\{}_{}.gdb\{}'.format(STATECODE, VINTAGE, STATECODE, VINTAGE,COUNTYNAME)
        ##output_path = r'D:\Jitendra\Data\SpatialData_2021\DMP_V21\AK.gdb\{}_{}_{}'.format(COUNTYNAME, STATECODE, VINTAGE)
        #output_path = r'D:\SpatialData_2021\DMP_V21\AK.gdb\{}'.format(COUNTYNAME)

        #arcpy.Copy_management(input_path, output_path, "Feature")
    logMessage(logFilePath, "Finished DMP geodatabase merging process.")
    logMessage(logFilePath, "********************************************************************************")
    print("Finished")