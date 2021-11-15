import csv,os,pandas
from pathlib import Path

PATH = '/home/data/online/asma/static/uploads/France_04.csv'
CHUNKS_awk = '"/home/data/online/asma/chunks/"'
CHUNKS = '/home/data/online/asma/chunks/'

def getFirstNameIndex(path):
    with open(path,encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for f in reader.fieldnames:
            if 'name' in f and 'first' in f:
                return reader.fieldnames, reader.fieldnames.index(f) + 1
                
def split(path):
    fields, c = getFirstNameIndex(path)
    os.system(r"""gawk -F, 'NR>1 {print > (""" + CHUNKS_awk + r""" substr($""" + str(c) + r""",1,2) ".csv")}' """+ path)
    files = os.listdir(CHUNKS)
    nonAlphabetic = []
    for file in files:
        data = []
        with open(CHUNKS+file , 'r', encoding="utf-8-sig") as rf:
            r = csv.reader(rf)
            data = [line for line in r]
        with open(CHUNKS+file , 'w') as hf:
            writer = csv.DictWriter(hf, fieldnames = fields, delimiter = ',')
            writer.writeheader()
            csv.writer(hf).writerows(data)
        name = Path(CHUNKS+file).stem.encode('utf-8', 'replace').decode()
        if not name.isalpha() or not name.isascii():
            nonAlphabetic.append(file)
    with open(CHUNKS+'extra.csv' , 'w') as fp:
        writer = csv.DictWriter(fp, fieldnames = fields, delimiter = ',')
        writer.writeheader()
        for f in nonAlphabetic:
            with open(CHUNKS+f , encoding="utf-8-sig") as tmp:
                for line in tmp:
                    fp.write(line)
            os.remove(CHUNKS+f)
        


