# coding: utf-8

import os
import json
import sqlite3
import time
import random
import M6
from Mobigen.Common import Log
from Mobigen.Common.Log import __LOG__
Log.Init()
import Mobigen.Common.Lock as Lock
import M6.Common.Default as Default
import M6.Common.TableIDTool as TableIDTool
import M6.Common.ToolBox as ToolBox

CREATE_SAMPLING_TABLE_QUERY = """
    CREATE TABLE SAMPLING_HISTORY (
            PARTITION_KEY   TEXT,
            PARTITION_DATE  TEXT,
            BLOCK_NUM       INTEGER, 

            NODE_ID         INTEGER,
            STATUS          TEXT,

            UNIQUE(PARTITION_KEY, PARTITION_DATE, BLOCK_NUM)
    )
"""

class MSD(object):
    def __init__(self, sock, param):
        object.__init__(self)
        self.sock = sock
        self.sid = '0_0'
        self.backendHash = {}
        self.WELCOME ="+OK Welcome MSD Server ver %s\r\n"% M6.VERSION

        self.fLock = Lock.CLock("%s/%s" % \
                (Default.M6_LOCK_DIR, "SYS_NODE_INFO_00000000000000"))

    def run(self):
        try :
            self.sock.SendMessage( self.WELCOME )
            if Default.DEBUG:
                __LOG__.Trace("Start %s" % str(self.sock.addr))

            while True:
                try :
                    line = self.sock.Readline(timeOut=Default.DLD_TIME_OUT).strip()
    
                    param_dic = json.loads(line)
                    cmd = param_dic['protocol'].upper()

                    # don't change this protocol order because of parameter dependency
                    if cmd == "QUIT":
                        self.sock.SendMessage("+OK BYE\r\n")
                        break
                    elif cmd == "INIT":
                        ret_message = self.INIT(param_dic)
                    elif cmd == "DEL":
                        ret_message = self.DEL(param_dic)
                    elif cmd == "SAMPLING_START":
                        ret_message = self.SAMPLING_START(param_dic)
                    else:
                        ret_message = "-ERR Invalid Command\r\n"

                    final_message = json.dumps(ret_message) + '\r\n'
                    self.sock.SendMessage(final_message)

                except self.sock.SocketDisconnectException :
                        break
                except Exception, err:
                    __LOG__.Exception()
                    #__LOG__.Exception(self.__class__.__name__, cmd)
                    ret_msg = "-ERR in MSD (%s)\r\n" % str(err)
                    self.sock.SendMessage(ret_msg)
                    break
        
            try : self.sock.close()
            except : pass
            if Default.DEBUG:
                __LOG__.Trace("End %s" % str(self.sock.addr))
        except :
            __LOG__.Exception()

   
    def INIT(self, param_dict):
        """
        샘플링 테이블 생성 

        @ request
        {
            "protocol": "init", 
            "database_name": "test_database",
            "table_name": "test_table"
        }

        @ response
        {
            "code" : 0,
            "message": ""
        }
        """
        # get parameter
        try:
            database_name = param_dict["database_name"]
            table_name = param_dict["table_name"]
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # default sampling_history directory 생성
        try: os.makedirs(Default.M6_MASTER_DATA_DIRs + '/sampling_history')
        except Exception, err: pass

        # 테이블 중복 체크
        if os.path.exists(Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_name)):
            return {"code": 0, "message": "-ERR %s sampling history is already exists" % table_name}

        # sampling history table 생성
        try:
            sampling_table_path = Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_name)
            conn = sqlite3.connect(sampling_table_path)
            cur = conn.cursor()
            cur.execute(CREATE_SAMPLING_TABLE_QUERY)
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message" : "-ERR Create %s table's sampling history fail" % table_name}
        finally:
            try: cur.close()
            except: pass
            try: conn.close()
            except: pass

        __LOG__.Trace("Create %s sampling history success" % table_name)
        return {"code": 0, "message" : "Create %s sampling history success" % table_name}

    def DEL(self, param_dict):
        """
        샘플링 테이블 삭제

        @ request
        {
            "protocol": "init", 
            "database_name": "test_database",
            "table_name": "test_table"
        }

        @ response
        {
            "code" : 0,
            "message": ""
        }
        """
        # get parameter
        try:
            database_name = param_dict["database_name"]
            table_name = param_dict["table_name"]
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_name)):
            return {"code": 0, "message": "-ERR %s sampling history is not exists" % table_name}

        # sampling history table 삭제
        try:
            os.remove(Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_name))
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message" : "-ERR Delete %s table's sampling history fail" % table_name}

        __LOG__.Trace("Delete %s sampling history success" % table_name)
        return {"code": 0, "message" : "Delete %s sampling history success" % table_name}
  
    def SAMPLING_START(self, param_dict):
        """
        샘플링 시작 
        - 블록 파일이 이전에 샘플링 되었는지 확인 
        - 블록파일이 C 상태인지 확인 
        - sampling history를 N 상태로 기록 

        @ request
        {
            "protocol": "sampling_start",
             "table_id": "T123"
             "partition_key": "k",
             "partition_date": "date",
             "block_num": 0
             "node_id": 3
        }

        @ response
        - 샘플링 해야하는 경우의 예
            {"code": 0, "message": "", "is_sampling_start": true}
        - 샘플링 하지 않아야할 경우의 예
            {"coce": 0, "message": "", "is_sampling_start": false}
        """
        # get parameter
        try:
            table_id = param_dict['table_id']
            partition_key = param_dict['partition_key']
            partition_date = param_dict['partition_date']
            balock_num = param_dict['block_num']
            node_id = param_dict['node_id']
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_name)):
            return {"code": 0, "message": "-ERR %s sampling history is not exists" % table_name}

        # sampling 여부 확인 
        # 1. 다중화된 블록 파일의 sampling 여부 확인 
        try:
            sampling_table_path = Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_id)
            conn = sqlite3.connect(sampling_table_path)
            cur = conn.cursor()
            cur.execute(CREATE_SAMPLING_TABLE_QUERY)
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message" : "-ERR Create %s table's sampling history fail" % table_name}
        finally:
            try: cur.close()
            except: pass
            try: conn.close()
            except: pass
        # sampling history table 삭제
        try:
            os.remove(Default.M6_MASTER_DATA_DIR + ('/sampling_history/%s.DAT' % table_name))
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message" : "-ERR Delete %s table's sampling history fail" % table_name}

        __LOG__.Trace("Delete %s sampling history success" % table_name)
        return {"code": 0, "message" : "Delete %s sampling history success" % table_name}

def test_2():
    from M6.Common.Protocol import Socket
    s = Socket()
    #msd = MSD(sock)

if __name__ == "__main__":
    from M6.Common.DB import Backend
    import M6.Common.Default as Default
    test_2()
