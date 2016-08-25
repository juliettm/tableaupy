# PostgreSQL -> Tableau
Get data from PostgreSQL query and push them into a Tableau Data Extract (.tde) file.

### Content
* create_extract.py: script to get data from PostgreSQL query and push them into a Tableau Data Extract (.tde) file.
* configuration.yaml: yaml file to import database configuration paramters.
* query.yaml: yaml file to import query parameters.

### Use
To create .tde file from query run:

```sh
$ python create_extract.py configuration.yaml query.yaml
```

### Notes
The data_types dictionary contains only a few data types to cast from python types to Tableau types. It is necessary to check that the types of data obtained from the query are included in the data_types dictionary, printing the column types (coltypes) may be helpful.

This is a very simple code, Python occasionally have encoding issues ;), it is necessary to take this into account in case of special characters into data.
One simple way is detect enconding using, for example: [chardet](https://github.com/chardet/chardet) before the insertion of string type data into .tde (insert_element function).

Code example:

```python
from chardet.universaldetector import UniversalDetector
def detect_encoding(element):
    detector = UniversalDetector()
    detector.feed(element)
    detector.close()
    if detector.result['encoding'] in [None, 'TIS-620',
            'windows-1255',  'windows-1253', 'windows-1251',
            'windows-1252', 'windows-1251', 'windows-1250',
            'KOI8-R', 'MacCyrillic', 'IBM855', 'IBM866',
            'EUC-KR', 'EUC-JP', 'SHIFT_JIS', 'CP932',
            'Big5', 'GB2312', 'EUC-TW', 'HZ-GB-2312']:
        code =  'utf-8'
    else:
        code = detector.result['encoding']
    return code

    # Use:
    # row.setCharString(pos,element.decode(str(detect_encoding(element))))
    # instead row.setCharString(pos,element)
    # in insert_element function.
```
