from M6.Common.Protocol import Socket
import json


def init_test():
    from M6.Common.Protocol import Socket
    s = Socket.Socket()
    s.Connect("localhost", 9999)

    print s.ReadMessage()
    param_dict = {}
    param_dict['protocol'] = 'sampling_end'
    param_dict['table_id'] = 'T8'
    param_dict['partition_key'] = 'k1'
    param_dict['partition_date'] = '20180103000000'
    param_dict['block_num'] = 2
    param_dict['node_id'] = 2

    param = json.dumps(param_dict) + '\n'

    s.SendMessage(param)
    print s.Readline()

if __name__ == "__main__":
    init_test()
