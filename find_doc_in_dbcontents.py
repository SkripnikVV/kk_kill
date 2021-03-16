import os, sys
# import colorama
from sysnsrc import SqLiteContents, title, printparam, printerror

if __name__ == '__main__':
    contents = SqLiteContents(dbpath=sys.argv[1])
    params = {}
    for i in range(2, len(sys.argv)):
        print(sys.argv[i])
        param = sys.argv[i].split('=')
        params[param[0]] = param[1]
    data = contents.find_doc(params)
    for d in data:
        print('\t'.join(d))
