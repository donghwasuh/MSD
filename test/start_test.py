from M6.Common.Protocol import Socket
import json


def init_test():
    from M6.Common.Protocol import Socket
    s = Socket.Socket()
    s.Connect("localhost", 9999)

    print s.ReadMessage()
    param_dict = {}
    param_dict['protocol'] = 'sampling_start'
    param_dict['table_id'] = 'T41'
    param_dict['partition_key'] = 'k7'
    param_dict['partition_date'] = '20190616000000'
    param_dict['block_num'] = 1
    param_dict['node_id'] = 1

    param = json.dumps(param_dict) + '\n'

    s.SendMessage(param)
    print s.Readline()

if __name__ == "__main__":
    init_test()
