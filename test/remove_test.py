from M6.Common.Protocol import Socket
import json


def init_test():
    from M6.Common.Protocol import Socket
    s = Socket.Socket()
    s.Connect("localhost", 9999)

    print s.ReadMessage()
    param_dict = {}
    param_dict['protocol'] = 'remove'
    param_dict['table_id'] = 'T8'
    param_dict['condition'] = '(PARTITION_KEY = \'k1\')'

    param = json.dumps(param_dict) + '\n'

    s.SendMessage(param)
    print s.Readline()

if __name__ == "__main__":
    init_test()
