from M6.Common.Protocol import Socket
import json


def init_test():
    from M6.Common.Protocol import Socket
    s = Socket.Socket()
    s.Connect("localhost", 9999)

    print s.ReadMessage()
    param_dict = {}
    param_dict['protocol'] = 'del'
    param_dict['database_name'] = 'test_database'
    param_dict['table_name'] = 'T8'

    param = json.dumps(param_dict) + '\n'

    s.SendMessage(param)
    print s.Readline()

if __name__ == "__main__":
    init_test()
