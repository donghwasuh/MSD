# coding: utf-8

import os
import json
import sqlite3
import time
import random
import threading
import datetime
import M6
from Mobigen.Common import Log
from Mobigen.Common.Log import __LOG__
Log.Init()
import Mobigen.Common.Lock as Lock
import M6.Common.Default as Default
import M6.Common.TableIDTool as TableIDTool
import M6.Common.ToolBox as ToolBox
from M6.Common.DB import Backend
from M6.Common.Protocol import Socket
from M6.Common.Protocol.DLDClient import Client as DLDClient

QUERY_FIND_NODE_SELECT = """
    SELECT
        i.NODE_ID, i.IP_ADDRESS_1, 'LOCAL', l.BLOCK_NUM,
        CASE WHEN
            l.STATUS not in ('D') and
            l.STATUS not like 'R%%' and    
            l.STATUS not like 'T%%'
            THEN i.SYS_STATUS
             ELSE 
            l.STATUS 
        END
    FROM
        (SELECT
            MAX(BLOCK_NUM) BLOCK_NUM
         FROM
            SYS_TABLE_LOCATION
         WHERE 1=1
            AND TABLE_KEY = '%(table_key)s'
            AND TABLE_PARTITION = '%(table_partition)s'
        ) r,
        (SELECT
            TABLE_KEY, TABLE_PARTITION, BLOCK_NUM, NODE_ID, STATUS
         FROM
            SYS_TABLE_LOCATION
         WHERE 1=1
            AND TABLE_KEY = '%(table_key)s'
            AND TABLE_PARTITION = '%(table_partition)s'
        ) l,
        SYS_NODE_INFO i
    WHERE 1=1
        AND l.NODE_ID = i.NODE_ID
        AND r.BLOCK_NUM = l.BLOCK_NUM
"""

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

CHECK_SAMPLING_HISTORY_QUERY = """
    SELECT 
        COUNT(*)
    FROM 
        SAMPLING_HISTORY
    WHERE
        %s
"""

SELECT_SAMPLING_HISTORY_WHERE_QUERY = """
    SELECT 
        %s
    FROM
        SAMPLING_HISTORY
    WHERE
        %s
"""

UPDATE_SAMPLING_HISTORY_STATUS_QUERY = """
    UPDATE 
        SAMPLING_HISTORY
    SET
        STATUS = '%s'
    WHERE
        PARTITION_KEY = '%s' AND
        PARTITION_DATE = '%s' AND
        BLOCK_NUM = %d AND
        NODE_ID = %d       
"""

INSERT_SAMPLING_HISTORY_QUERY = """
    INSERT INTO SAMPLING_HISTORY
        ( PARTITION_KEY, PARTITION_DATE, BLOCK_NUM, NODE_ID, STATUS)
    VALUES 
        ( '%s', '%s', %d, %d, '%s' )
"""

DELETE_SAMPLING_HISTORY_QUERY = """
    DELETE 
    FROM SAMPLING_HISTORY
    WHERE
        %s
"""

GET_SAMPLING_HISTORY_NODE_ID_QUERY = """
    SELECT  
        DISTINCT NODE_ID
    FROM
        SAMPLING_HISTORY
    WHERE
        %s
"""

SELECT_NODE_IP_QUERY = """
    SELECT
        IP_ADDRESS_1
    FROM
        SYS_NODE_INFO
    WHERE
        NODE_ID = %d

"""

SELECT_ALL_NODE_ID_QUERY = """
    SELECT
        NODE_ID 
    FROM
        SYS_NODE_INFO
    WHERE
        NODE_ID != 0

"""

SELECT_NODE_ID_QUERY = """
    SELECT
        NODE_ID
    FROM
        SYS_NODE_INFO
    WHERE
        IP_ADDRESS_1 = '%s'

"""

SYS_NODE_INFO = Default.M6_MASTER_DATA_DIR + "/SYS_NODE_INFO.DAT"
SYS_TABLE_LOCATION = Default.M6_MASTER_DATA_DIR + "/SYS_TABLE_LOCATION/%s/%s.DAT"
SAMPLING_HISTORY = Default.M6_MASTER_DATA_DIR + "/sampling_history/%s.DAT"

class MSD(object):
    def __init__(self, sock, param):
        object.__init__(self)
        self.sock = sock
        self.sid = '0_0'
        self.exp_check_thread = threading.Thread(target=self.expire_check)
        self.backendHash = {}
        self.WELCOME ="+OK Welcome MSD Server ver %s\r\n"% M6.VERSION

        self.fLock = Lock.CLock("%s/%s" % \
                (Default.M6_LOCK_DIR, "SYS_NODE_INFO_00000000000000"))

    def run(self):
        try :
            self.sock.SendMessage( self.WELCOME )
            if Default.DEBUG:
                __LOG__.Trace("Start %s" % str(self.sock.addr))

            self.exp_check_thread.start()
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
                    elif cmd == "SAMPLING_END":
                        ret_message = self.SAMPLING_END(param_dic)
                    elif cmd == "REMOVE":
                        ret_message = self.REMOVE(param_dic)
                    elif cmd == "REBUILD":
                        ret_message = self.REBUILD(param_dic)
                    elif cmd == "STATUS":
                        ret_message = self.STATUS(param_dic)
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

  
    def execute_query(self, path, query, is_select=True):
        data = []
        try:
            backend = Backend([path])
            conn = backend.GetConnection()
            cursor = conn.cursor()
            cursor.execute(query)
            if is_select:
                data = cursor.fetchall()
            else:
                conn.commit()
        except Exception, e:
            __LOG__.Exception()
            raise Exception(str(e))
        finally:
            try: cursor.close()
            except: pass
            try: conn.close()
            except: pass
        return data

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
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # default sampling_history directory 생성
        try: os.makedirs(Default.M6_MASTER_DATA_DIR + '/sampling_history')
        except Exception, err: pass

        # 테이블 중복 체크
        if os.path.exists(SAMPLING_HISTORY % table_name):
            return {"code": 1, "message": "-ERR %s sampling history is already exists" % table_name}

        try:
            sampling_table_path = SAMPLING_HISTORY % table_name
            conn = sqlite3.connect(sampling_table_path)
            cur = conn.cursor()
            cur.execute(CREATE_SAMPLING_TABLE_QUERY)
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message" : "-ERR Create %s table's sampling history fail" % table_name}
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
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(SAMPLING_HISTORY % table_name):
            return {"code": 1, "message": "-ERR %s sampling history is not exists" % table_name}

        # sampling history table 삭제
        try:
            os.remove(SAMPLING_HISTORY % table_name)
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message" : "-ERR Delete %s table's sampling history fail" % table_name}

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
            block_num = param_dict['block_num']
            node_id = param_dict['node_id']
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(SAMPLING_HISTORY % table_id):
            return {"code": 1, "message": "-ERR %s sampling history is not exists" % table_id}

        sampling_table_path = SAMPLING_HISTORY % table_id
        # sampling 여부 확인 
        # 1. 다중화된 블록 파일의 sampling 여부 확인 
        where_sql = "PARTITION_KEY = '%s' AND PARTITION_DATE = '%s' AND BLOCK_NUM = %d" % \
                    (partition_key, partition_date, block_num)
        try:
            result = self.execute_query(sampling_table_path, CHECK_SAMPLING_HISTORY_QUERY % where_sql)
            if result[0][0] != 0:
                return {"code": 1, "message" : "-ERR sampling history already exists"}
        except Exception, e:
            return {"code": 1, "message" : "-ERR Check %s table sampling history fail" % table_id}

        # 2. 블록 파일이 C 상태인지 확인
        mod_value = self.get_mode_value(table_id, partition_key, partition_date)
        ret = False
        for i in range(10):
            try:
                backend = self.get_backend(table_id, mod_value)
                cursor = backend.GetConnection().cursor()
                ret = True
            except Exception, err:
                __LOG__.Exception()
                return {"code": 1, "message" : "-ERR access to %s table backend fail" % table_id}
            if ret: break
            else: __LOG__.Watch("SAMPLING_START access backend [%s] retry %d" % (param_dict, i))

        sql = QUERY_FIND_NODE_SELECT % ({'table_key': partition_key, 'table_partition': partition_date})

        # block file의 상태가 C이고 현재 노드의 상태가 VALID인 경우에만 진행 
        ret = None
        for i in range(10):
            try:
                cursor.execute(sql)
                ret = cursor.fetchall()
                if ret[0][4] != 'VALID':
                    return {"code": 1, "message" : "-ERR block file status is not clean (%s)" %
                    param_dict}
            except Exception, err:
                __LOG__.Exception()
                return {"code": 1, "message" : "-ERR get block file status fail "}
            if ret == None:
                __LOG__.Watch("SAMPLING_START access backend [%s] retry %d" % (param_dict, i))
            else: break

        # 3. sampling history 기록 ( status = N )
        try:
            self.execute_query(sampling_table_path, INSERT_SAMPLING_HISTORY_QUERY % (partition_key,\
            partition_date, block_num, node_id, 'N'), False)
        except Exception, err:
            return {"code": 1, "message" : "-ERR Insert %s table sampling history fail" % table_id}

        __LOG__.Trace("Insert %s sampling history success \
                (key: %s, partition: %s, node_id : %d, block_num : %d)" \
                % (table_id, partition_key, partition_date, node_id, block_num))
        return {"code": 0, "message" : "Insert %s sampling history success \
                (key: %s, partition: %s, node_id : %d, block_num : %d)" \
                % (table_id, partition_key, partition_date, node_id, block_num)}

    def SAMPLING_END(self, param_dict):
        """
        블록 파일의 샘플링 종료 

        @ request
        {
            "protocol": "sampling_end",
             "table_id": "T123"
             "partition_key": "k",
             "partition_date": "date",
             "block_num": 0
             "node_id": 3
        }

        @ response
        {"code": 0, "message": ""}
        """
        # get parameter
        try:
            table_id = param_dict['table_id']
            partition_key = param_dict['partition_key']
            partition_date = param_dict['partition_date']
            block_num = param_dict['block_num']
            node_id = param_dict['node_id']
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(SAMPLING_HISTORY % table_id):
            return {"code": 1, "message": "-ERR %s sampling history is not exists" % table_id}

        sampling_table_path = SAMPLING_HISTORY % table_id

        # sampling 블록파일의 상태 변경 
        try:
            self.execute_query(sampling_table_path, UPDATE_SAMPLING_HISTORY_STATUS_QUERY %\
                    ('C', partition_key, partition_date, block_num, node_id), False)
        except Exception, err:
            return {"code": 1, "message" : "-ERR %s table history status update fail" % table_id}

        ret_message = "update %s sampling history success (%d, %s, %s, %d,)" % \
                (table_id, node_id, partition_key, partition_date, block_num)
        __LOG__.Trace(ret_message)
        return {"code" : 0, "message" : ret_message}

    def REMOVE(self, param_dict):
        """
        - sampling history에서 샘플링 정보 삭제 
        - woker에게 remove 정보 전달

        @ request
        {
            "protocol": "remove",
             "table_id": "T123"
             "condition": "(PARTITION_DATE >= '20180101000000')"
        }

        @ response
        {"code": 0, "message": ""}
        """
        
        # get parameter
        try:
            table_id = param_dict['table_id']
            condition = param_dict['condition']
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(SAMPLING_HISTORY % table_id):
            return {"code": 1, "message": "-ERR %s sampling history is not exists" % table_id}

        sampling_table_path = SAMPLING_HISTORY % table_id
        # 삭제하고자 하는 sampling 정보의 node_id 검색 
        try:
            result = self.execute_query(sampling_table_path, GET_SAMPLING_HISTORY_NODE_ID_QUERY % condition)
            node_list = []
            for row in result:
                node_list.append(row[0])
        except Exception, err:
            return {"code": 1, "message" : "-ERR %s table history status delete fail" % table_id}


        # FIXME
        # worker port 지정 
        # woker return 문에 따른 처리 
        for node_id in node_list:
            #try: node_ip = self.get_node_ip(node_id)
            #except Exception, e: return {"code" : 1, "message" : "-ERR get node_ip fail"}
            #port = 9999
            #s = Socket.Socket(node_ip, port)
            #
            #s.Readline()

            param_dict = {}
            param_dict['protocol'] = 'remove'
            param_dict['table_id'] = table_id
            param_dict['condition'] = self.change_column_name(condition)

            param = json.dumps(param_dict) + '\r\n'

            #ret_message = s.SendMessage(param)

        ## sampling history 삭제 
        try:
            self.execute_query(sampling_table_path, DELETE_SAMPLING_HISTORY_QUERY % condition, False)
        except Exception, err:
            return {"code": 1, "message" : "-ERR %s table history status delete fail" % table_id}

        ret_message = "delete %s sampling history success (delete condition : %s)" % \
                (table_id, condition)
        __LOG__.Trace(ret_message)
        return {"code" : 0, "message" : ret_message}

    def REBUILD(self, param_dict):
        """
        - sampling history에서 샘플링 정보 삭제 (Master의 REMOVE 이용)
        - woker에게 remove 정보 전달 (Master의 REMOVE 이용)
        - DLD정보를 이용하여 sampling history에 status(R)인 정보 넣음 
        - worker에게 rebuild 정보 전달 

        @ request
        {
            "protocol": "rebuild",
             "table_id": "T123"
             "condition": "(PARTITION_DATE >= '20180101000000')"
        }

        @ response
        {"code": 0, "message": ""}
        """
        
        # get parameter
        try:
            table_id = param_dict['table_id']
            condition = param_dict['condition']
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        # 테이블 존재 체크
        if not os.path.exists(SAMPLING_HISTORY % table_id):
            return {"code": 1, "message": "-ERR %s sampling history is not exists" % table_id}

        sampling_table_path = SAMPLING_HISTORY % table_id


        # sampling history 정보 삭제 
        ret_message = self.REMOVE(param_dict)
        if ret_message['code'] != 0:
            return ret_message
        

        # dld에서 condtion에 맞는 위치정보 select 
        l_query = self.change_column_name(condition, 'dld')
        with DLDClient() as client:
            result = client.FIND_NODE_SELECT(table_id, l_query)
            result.remove('+OK List\r\n')

        # DLD의 FIND_NODE_SELECT는 C상태인 블록 파일의 위치정보를 반환
        # ex ) ['ip_address, scope, key, partition, block_num\r\n']의 형태로 반환 
        # 다중화 파일이 존재하기 때문에 이중 하나의 위치정보만을 사용하여 rebuild 해야한다
        # 다중화된 블록의 위치정보중 하나의 블록만을 고르기 위해 dict로 구성
        # total_dict = {
        #     'key, partition, block_num' : ['ip_address, scope, key, partition, block_num\r\n', ...],
        #     'key1, partition1, block_num1' : ['ip_address, scope, key, partition, block_num\r\n', ...],
        # }
        # 
        # value에 존재하는 다중화된 위치정보중 1개를 랜덤으로 고른다 
        before_key = ''
        total_dict = {}
        for record in result:
            cur_key = record.strip().split(',', 2)[2]
            if before_key != cur_key:
                total_dict[cur_key] = []
                total_dict[cur_key].append(record.strip())
                before_key = cur_key
            else:
                total_dict[cur_key].append(record.strip())

        final_record_dict = {}
        target_record = ''
        for key in total_dict.keys():
            #  다중화된 DLD 위치정보 중 1개의 record를 random으로 고르는 작업 
            value_list = total_dict[key]
            target_record = value_list[random.randrange(0,len(value_list))]

           
            # DLD return value: (ip_address, scope, key, partition, block_num)를 
            # sampling_history : (key, partition, block_num, node_id, status) 변환
            ip_address, _, key, partition, block_num = target_record.split(',')
            
            try: node_id = self.get_node_id(ip_address)
            except Exception, e: return {"code": 1, "message" : "-ERR get node_id fail "}

            # data 변환 후 sampling history db에 insert
            try:
                self.execute_query(sampling_table_path, INSERT_SAMPLING_HISTORY_QUERY % \
                (key, partition, int(block_num), int(node_id), 'R'), False)
            except Exception, err:
                __LOG__.Exception()
                return {"code": 1, "message" : "-ERR Insert %s table sampling history fail" % table_id}
          
            # worker에게 전달하기 위해 node_id 별로 record 저장 
            # final_record_dict = {
            #           'ip_address_1' : ['key,partition,block_num', ...],
            #           'ip_address_2' : ['key,partition,block_num', ...],
            # }
            if not final_record_dict.has_key(ip_address):
                final_record_dict[ip_address] = []
            final_record_dict[ip_address].append(key + ',' + partition + ',' + block_num)

        print final_record_dict
        for keys in final_record_dict:
            worker_param_dic = self.make_rebuild_protocol(table_id, condition, final_record_dict[keys])
            print 'worker_param_dic : ', worker_param_dic
            # FIXME: rebuild 정보를 worker에 전달 , worker return에 따른 처리 추가 
            #port = 9999
            #s = Socket.Socket(keys, port)
            #s.Readline
            #ret_message = s.SendMessage(param_dict)

        ret_message = "%s sampling rebuild success  [%s]" % (table_id, param_dict)
        __LOG__.Trace(ret_message)
        return {"code" : 0, "message" : ret_message}

    def STATUS(self, param_dict):
        """
        - rebuild 상태 정보 전달
        - 각 노드의 블럭 크기 전달
        - 각 노드의 큐 정보 전달

        @ request
        {
            "protocol": "status",
             "table_id_list": "["T123"]"
        }

        @ response
        {"code": 0, "message": ""}
        """
        
        # get parameter
        try:
            table_id_list = eval(param_dict['table_id_list'])
        except Exception, err:
            __LOG__.Exception()
            return {"code": 1, "message": '-ERR Param error %s [%s]' % (str(err), param_dict)}

        try: node_id_list = self.get_all_node_id()
        except Exception, err: return {"code": 1, "message": '-ERR get node_ip fail'}

        print node_id_list

        #FIXME : worker로 status 프로토콜 전달 및 처리 

        for table_id in table_id_list:
            # 테이블 존재 체크
            sampling_table_path = SAMPLING_HISTORY % table_id
            if not os.path.exists(sampling_table_path):
                return {"code": 0, "message": "-ERR %s sampling history is not exists" % table_id}

            for node_id in node_id_list:
                remain_build_target = self.get_remain_rebuild_target(table_id, node_id)
                print 'rbt : ', remain_build_target
        return


    def make_rebuild_protocol(self, table_id, condition, record_list):
        param_dict = {}
        param_dict['protocol'] = 'rebuild'
        param_dict['table_id'] = table_id
        param_dict['condition'] = self.change_column_name(condition)

        param_dict['target_block'] = {}
        param_dict['target_block']['fields'] = ["partition_date", "partition_key", "block_num"]
        param_dict['target_block']['records'] = record_list
        return param_dict

    def get_remain_rebuild_target(self, table_id, node_id):
        query = """
            SELECT
                COUNT(*)
            FROM 
                SAMPLING_HISTORY
            WHERE
                NODE_ID = %d AND
                STATUS = 'R'
        """
        try:
            result = self.execute_query((SAMPLING_HISTORY % table_id), (query % node_id))
        except Exception, err:
            __LOG__.Exception()
            return {"code": 0, "message" : "-ERR Insert %s table sampling history fail" % table_id}
        return result[0][0]

        
    def get_node_ip(self, node_id):
        """
        node_id를 인자로 받아 node_ip를 반환 
        """
        try:
            result = self.execute_query(SYS_NODE_INFO, SELECT_NODE_IP_QUERY % node_id)
            node_ip = result[0][0]
        except Exception, err:
            __LOG__.Exception()
            raise Exception(err)
        return node_ip

    def get_node_id(self, node_ip):
        """
        node_ip를 인자로 받아 node_id를 반환 
        """
        try:
            result = self.execute_query(SYS_NODE_INFO, SELECT_NODE_ID_QUERY % node_ip)
            node_id = result[0][0]
        except Exception, err:
            __LOG__.Exception()
            raise Exception(err)
        return node_id

    def get_all_node_id(self):
        """
        현재 cluster의 모든 worker의 node_id를 list로 반환 
        """
        try:
            result = self.execute_query(SYS_NODE_INFO, SELECT_ALL_NODE_ID_QUERY)
            node_id_list = []
            for row in result:
                node_id_list.append(row[0])
        except Exception, err:
            __LOG__.Exception()
            raise Exception(err)
        return node_id_list

    def change_column_name(self, line, form='worker'):
        """
        woker로 column을 parameter로 전송시, column명 변경

        - form = worker, worker에게 전달하는 경우의 컬럼 이름 포맷 
        @ PARTITON_DATE -> _PARTITOIN_DATE
        @ PARTITIONE_KEY -> _PARTITION_KEY
        @ NODE_ID -> _NODE_ID
        
        - form = worker, DLD에게 전달하는 경우의 컬럼 이름 포맷 
        @ PARTITON_DATE -> TABLE_PARTITOIN
        @ PARTITIONE_KEY -> TABLE_KEY
        @ NODE_ID -> NODE_ID
        """
        if form == 'worker':
            if 'PARTITION_DATE' in line:
                return line.replace('PARTITION_DATE', '_PARTITION_DATE')
            elif 'PARTITION_KEY' in line:
                return line.replace('PARTITION_KEY', '_PARTITION_KEY')
            elif 'NODE_ID' in line:
                return line.replace('NODE_ID', '_NODE_ID')
        elif form == 'dld':
            if 'PARTITION_DATE' in line:
                return line.replace('PARTITION_DATE', 'TABLE_PARTITION')
            elif 'PARTITION_KEY' in line:
                return line.replace('PARTITION_KEY', 'TABLE_KEY')
            elif 'NODE_ID' in line:
                return line

    def get_backend(self, table_id, mod_val):
        """
        hash_mod_val로 나누어진 DLD 백엔드에 접근
        """
        backend_hash_key = table_id + 'M' + str(mod_val)
        if backend_hash_key not in self.backendHash:
            self.backendHash[backend_hash_key] = Backend([SYS_NODE_INFO,\
                SYS_TABLE_LOCATION % (table_id, mod_val)])
        
        backend = self.backendHash[backend_hash_key]
        return backend
    
    def get_mode_value(self, table_id, partition_key, partition_date):
        """
        hash 값을 기준으로 쪼개진 DLD block 파일에 접근하기 위해 
        mod value 계산 
        """
        hash_value = hash(table_id + partition_key + partition_date)
        hash_mod_val = ToolBox._get_hash_mod_value(table_id)
        mod_value = hash_value % hash_mod_val

        return mod_value

    def get_table_info(self, table_id):
        try:
            result = self.execute_query(Default.M6_MASTER_DATA_DIR + '/SYS_TABLE_INFO.DAT', \
                  "SELECT DSK_EXP_TIME FROM SYS_TABLE_INFO WHERE TABLE_NAME = '%s'" % table_id)
        except Exception, err:
            __LOG__.Exception()
            raise Exception(err)
        return result

    def expire_check(self):
        """
        - 테이블의 expire 시간 계산
        - sampling history에서 expire 시간 지난 레코드 삭제 
        - worker로 sampling data 삭제 요청 
        """

        while True:
            __LOG__.Trace("expire check thread: start...")
            # 현재시간 계산 
            dt = datetime.datetime.now()
            now_date_min =  datetime.datetime.strptime(dt.strftime("%Y%m%d%H%M%S"), '%Y%m%d%H%M%S')

            SAMPLING_HISTORY_DIR = Default.M6_MASTER_DATA_DIR + "/sampling_history/"
            file_list = os.listdir(SAMPLING_HISTORY_DIR)

            for file_name in file_list:
                # sampling_history directory에 존재하는 journal 파일 제외
                if 'journal' in file_name:
                    continue

                # table의 disk_exp_time 
                table_id = file_name[:-4]

                try: 
                    result = self.get_table_info(table_id)
                    if len(result) == 0:
                        __LOG__.Trace('expire checker thread: %s table is not exists' % table_id)
                        continue
                except Exception, e:
                    return {'code': 1, 'message': '-ERR %s' % str(e)}
                disk_exp_time = result[0][0]

                # (현재 시간 - disk_exp_time) 계산
                limit_date = now_date_min - datetime.timedelta(minutes=disk_exp_time)
                limit_time = datetime.datetime.strftime(limit_date, "%Y%m%d%H%M%S")

                __LOG__.Trace("expire check thread: try to delete \
                [table: %s, disk_exp_time : %s, limit_time: %s]..." \
                % (table_id, disk_exp_time, limit_time))
                # (현재 시간 - disk_exp_time) 이전 데이터 삭제를 위한 where 쿼리 
                condition_query = '(PARTITION_DATE <= \'%s\')' % limit_time

                # REMOVE 프로토콜을 사용하여 disk_exp 시간 지난 
                # sampling history 레코드 삭제 & worker로 sampling data 삭제 요청 
                param_dict = {}
                param_dict['protocol'] = 'remove'
                param_dict['table_id'] = table_id
                param_dict['condition'] = condition_query

                __LOG__.Trace("expire check thread: try to delete [%s]..." % param_dict)
                self.REMOVE(param_dict)

            __LOG__.Trace("expire check thread: sleep 60 seconds...")
            time.sleep(60)

        return None


def test_2():
    from M6.Common.Protocol import Socket
    s = Socket()
    #msd = MSD(sock)

if __name__ == "__main__":
    from M6.Common.DB import Backend
    import M6.Common.Default as Default
    test_2()
