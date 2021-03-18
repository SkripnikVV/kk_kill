'''
Скрипт открывает топик с утратой силы, переберает таблицу с реквизитами и ставит ссылки на тип
Параллельно с этим он ищет по оглавлению топики документов и справок, для утраты силы
На основе этих данных он генерирует комментарий об утрате силы и формирует патч для документов,
которые надо грохнуть
'''
# 25137158 25037158
# В справках после публикации (текст опубликован)
# !STYLE L 1 72 1
#  Текст постановления официально опубликован не был
import json
import re
import os
import time
import sysnsrc as sy

# contents = sy.SqLiteContents(dbpath=r'C:\projects\killer\kk.db')
contents = sy.SqLiteContents(dbpath=r'D:\utils\py\kk\kk.db')
#
typez = {'Постановление': 'Постановлением', 'Распоряжение': 'Распоряжением', 'Решение': 'Решением',
         'Приказ': 'Приказом', 'Указ': 'Указом', 'Закон': 'Законом'}

color = '''
ghbdtn

!STYLE J 0 73 5
     decor:{Font = { BackColor = clYellow }}yellow

!STYLE J 0 73 5
     decor:{Font = { BackColor = clRed }}red
'''

path1 = r'D:\utils\py\kk\400296598.nsr'
killdoc = sy.NsrDoc([])
killdoc.loadfromfile(path1)
topic = killdoc.gettopic()
print(topic)
killdate = killdoc.getcmd('!ACTIVE ')
if not killdate:
    killdate = killdoc.getcmd('!DATE ')
if killdate:
    killdate = killdate[0][2]
print(killdate)
killname = killdoc.getcmd('!NAME ')
killname = ' '.join([n[2] for n in killname]).strip()
killname = killname[:killname.find('"О')]
killname = killname.split()
if killname[0] in typez:
    killname[0] = typez[killname[0]]
killname = ' '.join(killname)
print(killname)
block = killdoc.getcmd('!BLOCK 999')
print(block)
table = killdoc.read_table(block[0][0])
print(table[0])
print(table[1])
new_table = table[2]
row = 0
col = 0
date_col = 4
code_col = 5
type_col = 3
name_col = 2
skip = 0
value = []
patch = {}
for index, line in enumerate(new_table):
    if skip > 0:
        skip -= 1
        continue
    if re.search('^!ROW\s*$', line):
        row += 1
        col = 0
        date = None
        code = None
        d_type = None
        d_name = None
        lnk_index = None
        sub = None
    elif re.search('^!(BLOCK|SUB)\s+(\d+)', line):
        rsm = re.search('!(BLOCK|SUB)\s+(\d+)', line)
        if rsm:
            sub = rsm[2]
    elif re.search('^!CELL\s+', line):
        col += 1
    elif len(line) > 0 and line[0] != '!':
        if col == code_col:
            code = line.strip().upper()
        if col == date_col:
            date = line.strip().upper().replace('.', '/')
        if col == type_col:
            d_type = line
            lnk_index = index
        if col == name_col:
            name = line
    elif re.search('!ROWEND', line) and date and code:
        relive = name
        killed_doc = contents.find_doc({'date': date, 'code': code, 'relevation': relive, 'crop_garant_name': True})
        if killed_doc:
            print(killed_doc[0])
            try:
                killed_name = killed_doc[0][3]
            except Exception:
                print(Exception)
                continue
            # print(killed_name)
            killed_name = killed_name[:killed_name.find('"О')]
            # time.sleep(4)
            patchdata = {'killtopic': topic[0], 'killsub': sub, 'killdate': killdate, 'killname': killname,
                         'killed_name': killed_name, 'killed_topic': killed_doc[0][0]}
            if re.search('\((утратил|отменен|документ\s+утратил)', killed_doc[0][3]):
                patchdata['double'] = True
            patchdata['related'] = killed_doc[0][4]
            cmt = sy.generatekillcmt(patchdata)
            fuscmt = sy.generate_fus_cmt(patchdata)
            patchdata['cmt'] = cmt
            patchdata['fuscmt'] = fuscmt
            new_table[lnk_index] = chr(4) + new_table[lnk_index].strip() + chr(4) + killed_doc[0][0] + chr(4) + '\n'
            percent = str(killed_doc[0][6])[:5]
            print(percent)
            if killed_doc[0][-2] > 85:
                color = 'decor:{Font = { BackColor = clLime }}'
            elif 60 < killed_doc[0][-2] < 85:
                color = 'decor:{Font = { BackColor = clYellow }}'
            else:
                color = 'decor:{Font = { BackColor = clRed }}'
            value = ['!CELL 5000 1111 0 0',
                     '!STYLE L 1 72 1', f'\x01{color}' + percent + ' ' + killed_doc[0][3].strip() + '\x01',
                     '!STYLE L 1 72 1', ';' + cmt,
                     '!CELLEND\n']
            # print(value)
            new_table.insert(index, '\n'.join(value))
            skip = 1
            patch[killed_doc[0][0].strip()] = patchdata
        else:
            value = ['!CELL 5000 1111 0 0', '!STYLE L 1 72 1', '', '!CELLEND\n']
            new_table.insert(index, '\n'.join(value))
            skip = 1
        # print(skip)

with open(path1 + '.new.nsr', 'w', encoding='cp866') as fl:
    fl.writelines(new_table)

killdoc.doc = killdoc.doc[:table[0]] + new_table + killdoc.doc[table[1]:]
with open(os.path.split(path1)[0] + f'\\{topic[0]}-modify.nsr', 'w', encoding='cp866') as fl:
    fl.writelines(killdoc.doc)

with open(os.path.split(path1)[0] + '\patch.json', 'w', encoding='UTF-8') as fl:
    fl.write(json.dumps(patch, indent=2, ensure_ascii=False))

