import sys, os
import json
import colorama

import sysnsrc as sy

if __name__ == '__main__':
    colorama.init()
    script_folder, script_file = os.path.split(sys.argv[0])
    sy.title(sys.argv[0], 'Скрипт ищет документы из текстового файла по оглавлению contents.db')

    try:
        l_path = sys.argv[1]
        db_path = sys.argv[3]
        if sys.argv[2].find('1251') > -1:
            encoding = 'cp1251'
        elif sys.argv[2].find('866') > -1:
            encoding = 'cp866'
        else:
            encoding = 'UTF-8'
        print(l_path, db_path, encoding)
        contents = sy.SqLiteContents(dbpath=db_path)
        print('connected')
        with open(l_path, 'r', encoding=encoding) as fl:
            lines = fl.readlines()
        print('len(lines)', len(lines))
    except Exception:
        sy.printerror('Ошибка', 'неверный вызов')
        print(Exception)
        sy.printparam('Формат вызова', ':')
        sy.printparam(script_file, r'"путь\к файлу\со списком.txt" [КОДИРОВКА] "путь к базе\данных\contents.db"')
        exit()
    find_data = []
    for line in lines:
        line = sy.replaceallstrdate(line)

        parameters = {'text': line, 'crop_garant_name': True}
        ddt = sy.get_date(line)
        if ddt:
            parameters['date'] = ddt
        dcd = sy.get_code(line)
        if dcd:
            parameters['code'] = dcd.upper()
        selfname = sy.crop_garant_name(line)
        if selfname:
            parameters['relevation'] = selfname.upper()

        find_data.append(parameters)

    # with open(os.path.split(l_path)[0]+'0.json', 'w', encoding='UTF-8') as fl:
    #     fl.write(json.dumps(find_data, indent=2, ensure_ascii=False))
    find_data = contents.find_many_doc(find_data)
    report = sy.Excelreport('РЕЗУЛЬТАТ ПОИСКА')
    report.addheaders(['ЧТО ИСКАЛИ:', 'СТЕПЕНЬ СООТВЕТСТВИЯ', 'MAIN', 'TOPIC', 'RELATED', 'GARANT NAME', 'SOURCE', 'WARNING'])
    for item in find_data:
        if item.get('topic'):
            revel = str(item.get('relevation', 0.0))[:5]
        else:
            revel = ''
        report.addrow([
            item['text'],
            revel,
            item.get('main', ''),
            item.get('topic', ''),
            item.get('related', ''),
            item.get('name', ''),
            item.get('src', ''),
            item.get('warning', ''),
        ])
    report.save(os.path.split(l_path)[0] + '\\result.xlsx')
    print(os.path.split(l_path)[0] + '\\result.xlsx')
    # with open(os.path.split(l_path)[0]+'1.json', 'w', encoding='UTF-8') as fl:
    #     fl.write(json.dumps(find_data, indent=2, ensure_ascii=False))
