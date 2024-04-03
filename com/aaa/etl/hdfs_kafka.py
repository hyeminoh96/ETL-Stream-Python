from pyarrow import fs

class Hdfs2Kafka(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self._hdfs = fs.HadoopFileSystem('localhost', port=9000)
        
    def getHdFileInfo(self, filename):
        f_Info = self._hdfs.get_file_info(filename) 
        print('파일 종류: ', str(f_Info.type))
        print('파일 경로: ', str(f_Info.path))
        print('파일 크기: ', str(f_Info.size))
        print('파일 수정일자: ', str(f_Info.mtime))
        
    def readHdFile(self, filename):
        with self._hdfs.open_input_file(filename) as inf:
            read_data = inf.read().decode('utf-8').splitlines()
            new_line = [line.split(",") for line in read_data]
            return new_line
        
    def sendData2Kafka(self, topic, list_line):
        for data in list_line:
            str_tmp = ",".join(data).split(",")
            modified_data = ",".join(str_tmp[:2]) + "," + ",".join(str_tmp[4:])
            print(modified_data)