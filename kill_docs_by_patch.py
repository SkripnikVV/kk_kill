import datetime
import json
import os
import sys
import re
import logging
import sysnsrc as sy


def get_version():
    cyrdate = datetime.datetime.today()
    middle = cyrdate.isoweekday()
    while cyrdate.isoweekday() != 6:
        cyrdate += datetime.timedelta(days=1)
    if middle > 3:
        cyrdate += datetime.timedelta(days=7)
    return cyrdate.strftime('%d/%m/%Y')


def patch_related(key, data, path, fuslog, patch):
    modify = False
    reltopic = data.get('related')
    if not reltopic:
        logging.error('не найден топик справки для: ' + key)
        return
    doc = sy.NsrDoc([])
    doc.loadfromfile(path + f'\\{reltopic}.nsr')
    skip = 0
    changecount = 0
    lastbreakindex = 0
    for index, line in enumerate(doc.doc):
        if skip > 0:
            skip -= 1
            continue
        if line.strip() == '':
            lastbreakindex = index
        if changecount != 0 and line.strip() != '' and line[0] != '!' and line.find(chr(4)) > -1:
            fuslnk = re.search(r'\x04[^\x04]+\x04(\d+)', line)
            if not fuslnk[1] in patch.keys():
                logging.info('проверить ФУС:\t' + line)
                fuslog.write('-' * 30 + '\n')
                fuslog.write(f'{line.split(chr(4))[2]}\n{data["fuscmt"]}\n')
            else:
                logging.info('ПРОПУСК ФУС. ДОКУМЕНТ УТРАЧИВАЮТ ТЕКУЩИМ ПАТЧЕМ:\t' + line)
        if re.search('В\s+настоящий\s+документ\s+внесены\s+изменения\s+следующими\s+документами', line):
            doc.doc.insert(index - 1, f'!STYLE L 1 72 1\n{data["cmt"]}\n\n')
            modify = True
            changecount += 1
            skip = 1
            logging.info('вставили перед информацией об изменениях')

    if changecount == 0:
        doc.doc.insert(lastbreakindex, f'\n!STYLE L 1 72 1\n{data["cmt"]}\n')
        logging.info('вставили в конце справки')
        modify = True
    if modify:
        doc.saveasfile(path + '\\import' + f'\\{reltopic}-modify.nsr')
    logging.info('save')


def patch_document(key, data, path):
    modify = False
    logging.info('load: ' + key)
    doc = sy.NsrDoc([])
    doc.loadfromfile(path + f'\\{key}.nsr')
    ins_index = 5
    belongs = doc.getcmd('!BELONGS')
    if belongs:
        ins_index = belongs[0][0] + 1

    # устанавливаем warning
    warning = doc.getcmd('!WARNING')
    if not warning:
        doc.doc.insert(ins_index, '!WARNING 2\n')
    else:
        logging.info('ЗАМЕНА WARNING: ' + doc.doc[warning[0][0]] + 'на !WARNING 2')
        doc.doc[warning[0][0]] = '!WARNING 2\n'

    if data.get('double'):
        doc.doc.insert(ins_index,
                       r'!*PUBLISHEDIN Автоматизация\УТРАТА СИЛЫ (ПОВТОРНО)|' + datetime.datetime.today().strftime(
                           '%d/%m/%Y') + '|\n')
    else:
        doc.doc.insert(ins_index, r'!*PUBLISHEDIN Автоматизация\УТРАТА СИЛЫ|' + datetime.datetime.today().strftime(
            '%d/%m/%Y') + '|\n')

    doc.doc.insert(ins_index, '!VABOLISHED ' + get_version() + '\n')

    # модифицируем active
    if not doc.getcmd('!NOACTIVE'):
        active = doc.getcmd('!ACTIVE ')
        if not active:
            doc.doc.insert(ins_index, '!NOACTIVE\n')
        else:
            # если эктив НЕ ЗАКРЫТ
            if active[0][2].find('-') == -1:
                start_active = datetime.datetime.strptime(active[0][2], '%d/%m/%Y')
                stop_active = datetime.datetime.strptime(data['killdate'], '%d/%m/%Y')
                if start_active > stop_active:
                    logging.error(
                        'ЗАКРЫВАЮЩИЙ ЭКТИВ МЕНЬШЕ ОТКРЫВАЮЩЕГО. ПРИМЕНИЛИ НОЭКТИВ: ' + active[0][2] + ' > ' + data[
                            'killdate'])
                    doc.doc[active[0][0]] = '!NOACTIVE\n'
                else:
                    stop_active -= datetime.timedelta(days=1)
                    doc.doc[active[0][0]] = doc.doc[active[0][0]].strip() + '-' + stop_active.strftime(
                        '%d/%m/%Y') + '\n'
                    logging.info(
                        'ЗАКРЫЛИ ЭКТИВ: ' + doc.doc[active[0][0]])

    # устанавливаем name
    name = doc.getcmd('!NAME')
    if name:
        # удалим старый нейм
        index = name[0][0]
        for _ in range(len(name)):
            doc.doc.pop(index)
        newname = ' '.join([n[2] for n in name])
        newname = re.sub(
            '\((утратил[а-я]*\s+силу|не\s+действует|отменен[а-я]*|прекратил[а-я]*\s+действие|документ\s+утратил\s+силу)\)',
            '', newname)
        newname = '!NAME ' + newname + ' (документ утратил силу)\n'
        doc.doc.insert(index, newname)
    logging.info(newname)
    skipindex = 0
    for index, line in enumerate(doc.doc):
        if skipindex > 0:
            skipindex -= 1
            continue
        if line.find('!STYLE J 1 72 1') > -1:
            logging.info('вставка пропустила: ' + line)
            continue
        if re.search('^!(BLOCK|STYLE L|STYLE J|STYLE V|STYLE R|STYLE P)', line):
            doc.doc.insert(index, f'!STYLE J 1 72 1\n\x05\x03{data["cmt"]}\x03\x05\n\n')
            modify = True
            logging.info('insert after: ' + line)
            break
    if modify:
        doc.saveasfile(path +'\\import' + f'\\{key}-modifyD.nsr')
    logging.info('save')


if __name__ == '__main__':
    if len(sys.argv) == 0:
        basefolder = os.getcwd()
    else:
        basefolder = sys.argv[1]
    if not os.path.exists(basefolder):
        sy.printerror('отсутствует базовая папка:', basefolder)
        exit(1)
    fuslog = open(basefolder + '\\проверить на ФУС.txt', 'w', encoding='UTF-8')
    logging.basicConfig(filename=basefolder + '\\kill_patch.log', filemode='w', level=logging.INFO)
    with open(basefolder + '\\patch.json', 'r', encoding='UTF-8') as fl:
        patch = json.loads(fl.read())
        logging.info(f'load patch: {len(patch)}')

    if not patch:
        print('Не нашли patch')
        logging.error(f'load patch: {len(patch)}')
        exit(1)

    if not os.path.exists(basefolder + '\\import'):
        try:
            os.makedirs(basefolder + '\\import')
        except:
            exit(1)

    logging.info('переберем документы:')
    for key, value in patch.items():
        print(key)
        if os.path.exists(basefolder + f'\\{key}.nsr'):
            patch_related(key, value, basefolder, fuslog, patch)
            patch_document(key, value, basefolder)
        else:
            logging.info(f'топик {key} не найден')
            print('skip')
    fuslog.close()
