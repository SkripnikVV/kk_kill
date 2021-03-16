import os, sys
import re
import colorama
import sysnsrc as sy


def get_id(text):
    if text.find(' - ') > -1:
        text = text[:text.find(' - ')]
    return ''.join([s for s in text if s.isdigit()])


def add_link(file):
    pattern = '(Строки\s+\d+)(\s+\-\s+[\d|\.]+)'

    doc = sy.NsrDoc([])
    doc.loadfromfile(file)
    for index, line in enumerate(doc.doc):
        rsm = re.search(pattern, line)
        if rsm:
            doc.doc[index] = re.sub(pattern, '\x04\g<0>\x0447401210.' + get_id(rsm[1]) + '\x04', doc.doc[index])
            print(rsm[0])
    doc.saveasfile(file + '.mod.nsr')


def replace_link_by_list(file):
    links = set(
        ['47401210.15898', '47401210.15899', '47401210.15900', '47401210.15901', '47401210.15902', '47401210.15955',
         '47401210.15967', '47401210.15972', '47401210.15973', '47401210.15975', '47401210.15976', '47401210.15977',
         '47401210.16015', '47401210.16019', '47401210.16020', '47401210.16023', '47401210.16024', '47401210.16027',
         '47401210.16038', '47401210.16039', '47401210.16041', '47401210.16042', '47401210.16045', '47401210.16052',
         '47401210.16082', '47401210.16089', '47401210.16090', '47401210.16096', '47401210.16099', '47401210.16110',
         '47401210.16123', '47401210.16128', '47401210.16132', '47401210.16134', '47401210.16140', '47401210.16142',
         '47401210.16326', '47401210.16333', '47401210.16334', '47401210.16336', '47401210.16337', '47401210.16368',
         '47401210.16399', '47401210.16401', '47401210.16466', '47401210.16484', '47401210.16500', '47401210.16503',
         '47401210.16506', '47401210.16528', '47401210.16531', '47401210.16532', '47401210.16540', '47401210.16543',
         '47401210.16558', '47401210.16567', '47401210.16619', '47401210.16712', '47401210.16766', '47401210.16812',
         '47401210.16843', '47401210.16844', '47401210.16946', '47401210.16948', '47401210.16995', '47401210.17013',
         '47401210.17019', '47401210.17020', '47401210.17024', '47401210.17037', '47401210.17067', '47401210.17072',
         '47401210.17095', '47401210.17123', '47401210.17145', '47401210.17156', '47401210.17181', '47401210.17241',
         '47401210.17282', '47401210.17287', '47401210.17313', '47401210.17353', '47401210.17355', '47401210.17384',
         '47401210.17420', '47401210.17422', '47401210.17425', '47401210.17431', '47401210.17435', '47401210.17441',
         '47401210.17462', '47401210.17512', '47401210.17521', '47401210.17611', '47401210.17642', '47401210.17649',
         '47401210.17696', '47401210.17701', '47401210.17753', '47401210.17760', '47401210.17846', '47401210.17900',
         '47401210.17906', '47401210.17908', '47401210.17919', '47401210.17924', '47401210.17926', '47401210.17928',
         '47401210.17942', '47401210.17948', '47401210.17950', '47401210.17953', '47401210.17963', '47401210.17964',
         '47401210.17965', '47401210.17966', '47401210.17975', '47401210.17984', '47401210.17997', '47401210.18000',
         '47401210.18005', '47401210.18006', '47401210.18008', '47401210.18010', '47401210.18044', '47401210.18045',
         '47401210.18066', '47401210.18074', '47401210.18079', '47401210.18093', '47401210.18118', '47401210.18120',
         '47401210.18121', '47401210.18125', '47401210.18128', '47401210.18142', '47401210.18156', '47401210.18166',
         '47401210.18168', '47401210.18181', '47401210.18188', '47401210.18194', '47401210.18195', '47401210.18208',
         '47401210.18210', '47401210.18238', '47401210.18285', '47401210.18298', '47401210.18300', '47401210.18316',
         '47401210.18335', '47401210.18341', '47401210.18348', '47401210.18350', '47401210.18352', '47401210.18357',
         '47401210.18364', '47401210.18367', '47401210.18368', '47401210.18372', '47401210.18376', '47401210.18383',
         '47401210.18384', '47401210.18439', '47401210.18440', '47401210.18441', '47401210.18450', '47401210.18453',
         '47401210.18457', '47401210.18464', '47401210.18522', '47401210.18540', '47401210.18562', '47401210.18564',
         '47401210.18580', '47401210.18584', '47401210.18601', '47401210.18615', '47401210.18621', '47401210.18629',
         '47401210.18630', '47401210.18661', '47401210.18663', '47401210.18665', '47401210.18671', '47401210.18682',
         '47401210.18690', '47401210.18772', '47401210.18780', '47401210.18816', '47401210.18823', '47401210.18832',
         '47401210.18836', '47401210.18841', '47401210.18843', '47401210.18853', '47401210.18874', '47401210.18879',
         '47401210.18883', '47401210.18886', '47401210.18888', '47401210.18942', '47401210.18951', '47401210.19038',
         '47401210.19049', '47401210.19051', '47401210.19117', '47401210.19131', '47401210.19168', '47401210.19188',
         '47401210.19192', '47401210.19231', '47401210.19245', '47401210.19254', '47401210.19255', '47401210.19290'])

    def fix(rsm):
        if rsm[2] in links and rsm[2].find('.') > -1:
            print(rsm[0], end='\t')
            topic, sub = rsm[2].split('.')
            sub = get_id(rsm[1])
            print(f'\x04{rsm[1]}\x04{topic}.{sub}\x04')
            return f'\x04{rsm[1]}\x04{topic}.{sub}\x04'
        else:
            return rsm[0]

    doc = sy.NsrDoc([])
    doc.loadfromfile(file)
    for index, line in enumerate(doc.doc):
        doc.doc[index] = re.sub(r'\x04([^\x04]+)\x04([^\x04]+)\x04', fix, line)

    doc.saveasfile(file + '.mod.nsr')


def add_sub(file):
    # 47401210.1000

    pattern = '(Строки\s+\d+)(\s+\-\s+[\d|\.]+)'

    doc = sy.NsrDoc([])
    doc.loadfromfile(file)
    cmd = doc.getcmd('!BLOCK 999')
    print(cmd)
    table = doc.read_table(cmd[0][0])
    new_table = table[2]
    skip = 0
    stylecount = 0
    styleindex = 0
    laststyle = ''
    row, col = 0, 0
    h1index = '777'
    for i in range(len(new_table) - 1, -1, -1):
        if new_table[i].find('!SUB ') > -1:
            delete = new_table.pop(i)
            print('pop', delete)
    for index, line in enumerate(new_table):
        if skip > 0:
            skip -= 1
            continue
        if re.search('^!ROW\s+', line):
            row += 1
            col = 0
        elif re.search('^!CELL\s+', line):
            col += 1
            stylecount = 0
        elif re.search('^!STYLE\s+', line):
            stylecount += 1
            if stylecount == 1 and col == 1:
                styleindex = index
                laststyle = line.strip()
        elif len(line) > 0 and line[0] != '!' and re.search('\s*\d+', line):
            if col == 1:
                sub = get_id(line)
                if laststyle == '!STYLE #3':
                    value = f'!SUB {h1index}{sub}\n'
                else:
                    value = f'!SUB {sub}\n'
                print(value)
                new_table.insert(styleindex, value)
                skip = 1
    doc.doc = doc.doc[:table[0]] + new_table + doc.doc[table[1]:]
    doc.saveasfile(file + '.mod.nsr')


def replace_sub_by_dict(file):
    data1 = {'Пункт 11.2': '1112', 'Пункт 11.3': '1113', 'Пункт 11.4': '1114', 'Пункт 13': '1013', 'Пункт 14': '1014',
             'Пункт 15': '1015', 'Пункт 16': '1016', 'Пункт 17': '1017', 'Пункт 18': '1018', 'Пункт 19': '1019',
             'Пункт 19.3': '1193', 'Пункт 2': '1002', 'Пункт 21': '1121', 'Пункт 22': '1022', 'Пункт 23': '1123',
             'Пункт 24': '1124', 'Пункт 25': '1125', 'Пункт 26': '1126', 'Пункт 27': '1127', 'Пункт 28': '1128',
             'Пункт 29': '1129', 'Пункт 3': '1003', 'Пункт 30': '1130', 'Пункт 31': '1131', 'Пункт 35': '1035',
             'Пункт 36': '1036', 'Пункт 37': '1037', 'Пункт 39': '1039', 'Пункт 4': '1104', 'Пункт 40': '1040',
             'Пункт 41': '1041', 'Пункт 46.1': '1461', 'Пункт 47': '1047', 'Пункт 48': '1000000148', 'Пункт 5': '1005',
             'Пункт 50': '1050', 'Пункт 51': '1051', 'Пункт 52': '1152', 'Пункт 53': '1053', 'Пункт 54': '1054',
             'Пункт 55': '1055', 'Пункт 56': '1056', 'Пункт 57': '1057', 'Пункт 58': '1058', 'Пункт 59': '1059',
             'Пункт 6': '1006', 'Пункт 60': '1060', 'Пункт 61': '1061', 'Пункт 62': '1062', 'Пункт 63': '1063',
             'Пункт 64': '1064', 'Пункт 7': '1007', 'Пункт 8': '1008'}

    data2 = {'Пункт 10': '2010', 'Пункт 11': '2011', 'Пункт 12': '2012', 'Пункт 13': '2013', 'Пункт 14': '2014',
             'Пункт 15': '2015', 'Пункт 16': '2016', 'Пункт 17': '2017', 'Пункт 18': '2018', 'Пункт 19': '2019',
             'Пункт 2': '2002', 'Пункт 20': '2020', 'Пункт 21': '2021', 'Пункт 22': '2022', 'Пункт 23': '2023',
             'Пункт 24': '2024', 'Пункт 25': '2025', 'Пункт 26': '2026', 'Пункт 27': '2027', 'Пункт 28': '2028',
             'Пункт 29': '2029', 'Пункт 3': '2003', 'Пункт 30': '2030', 'Пункт 31': '2031', 'Пункт 32': '2032',
             'Пункт 33': '2033', 'Пункт 34': '2034', 'Пункт 4': '2004', 'Пункт 44': '1044', 'Пункт 5': '2005',
             'Пункт 6': '2006', 'Пункт 7': '2007', 'Пункт 8': '2008', 'Пункт 9': '2009'}

    data3 = {'Пункт 1': '10201', 'Пункт 10': '102010', 'Пункт 11': '102011', 'Пункт 12': '102012', 'Пункт 14': '102014',
             'Пункт 15': '102015', 'Пункт 16': '102016', 'Пункт 17': '102017', 'Пункт 18': '102018',
             'Пункт 19': '102019', 'Пункт 2': '10202', 'Пункт 20': '102020', 'Пункт 21': '102021', 'Пункт 22': '102022',
             'Пункт 23': '102023', 'Пункт 24': '102024', 'Пункт 25': '102025', 'Пункт 26': '102026',
             'Пункт 27': '102027', 'Пункт 29': '102029', 'Пункт 3': '10203', 'Пункт 30': '102030', 'Пункт 31': '102031',
             'Пункт 32': '102032', 'Пункт 33': '102033', 'Пункт 34': '102034', 'Пункт 35': '102035',
             'Пункт 36': '102036', 'Пункт 37': '102037', 'Пункт 38': '102038', 'Пункт 39': '102039', 'Пункт 4': '10204',
             'Пункт 40': '102040', 'Пункт 41': '102041', 'Пункт 42': '102042', 'Пункт 43': '102043',
             'Пункт 44': '102044', 'Пункт 45': '102045', 'Пункт 46': '102046', 'Пункт 47': '102047',
             'Пункт 48': '102048', 'Пункт 49': '102049', 'Пункт 5': '10205', 'Пункт 50': '102050', 'Пункт 51': '102051',
             'Пункт 52': '102052', 'Пункт 53': '102053', 'Пункт 54': '102054', 'Пункт 55': '102055', 'Пункт 6': '10206',
             'Пункт 7': '10207', 'Пункт 8': '10208', 'Пункт 9': '10209'}
    log = []
    data = data3

    def delete_subs(new_table):
        index = 0
        col = 0
        row = 0
        while index < len(new_table) - 1:
            if re.search('^!ROW\s+', new_table[index]):
                row += 1
                col = 0
            elif re.search('^!CELL\s+', new_table[index]):
                col += 1
            elif re.search('!SUB \d+', new_table[index]) and col > 1:
                delete = new_table.pop(index)
                print('del:', delete)
                continue
            index += 1
        return new_table

    doc = sy.NsrDoc([])
    doc.loadfromfile(file)
    topic = doc.gettopic()[0]
    cmd = doc.getcmd('!BLOCK 1401253')
    print(cmd)
    table = doc.read_table(cmd[0][0])
    new_table = table[2]
    print('new_table:', len(new_table))
    count = 0
    new_table = delete_subs(new_table)
    for index, line in enumerate(new_table):
        rsm = re.search(r'^!SUB (\d+)\s+(.+)', line)
        if rsm:
            log.append(rsm[0] + '\n')
            if data3.get(rsm[2].strip()):
                count += 1
                new_table[index] = '!SUB ' + data3.get(rsm[2].strip()) + ' ' + rsm[2] + '\n'
                log.append(f'{rsm[2].strip()}\t{data3.get(rsm[2].strip())}\n')
    print(count, len(data3.keys()))
    doc.doc = doc.doc[:table[0]] + new_table + doc.doc[table[1]:]

    doc.doc.append('!STYLE P 0 73 0\n')
    for key, value in data3.items():
        doc.doc.append(f';\x04{key} приложения 1\x04{topic}.{value}\x04\n')

    doc.doc = doc.doc[:table[0]] + new_table + doc.doc[table[1]:]

    doc.saveasfile(file + '.mod.nsr')
    with open(file + '.log', 'w') as fl:
        fl.writelines(log)


if __name__ == '__main__':
    colorama.init()
    sy.title(sys.argv[0], 'Модификация НСР документа')
    mode = sys.argv[1]
    file = sys.argv[2]
    if mode == 'add_link':
        add_link(file)
    elif mode == 'repl_link':
        replace_link_by_list(file)
    elif mode == 'add_sub':
        add_sub(file)
    elif mode == 'repl_sub':
        replace_sub_by_dict(file)
    else:
        print('неизвестная команда')