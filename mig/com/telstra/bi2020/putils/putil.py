import os

def createFolder(path):
    #print 'hello 1'
    if not os.path.exists(path):
        os.makedirs(path)

def writeFile(path,fileName,content):
    with open(path + fileName, "w") as text_file:
        text_file.write(content)
        text_file.close()