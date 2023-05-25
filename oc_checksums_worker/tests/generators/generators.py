import random
import tempfile
import os
import zipfile
import string

def generate_version(comps):
    _comps = list()

    while len(_comps) < comps:
        _comps.append('%d' % random.randint(0, 99))

    return '.'.join(_comps)

def generate_str(len_min=None, len_max=None, add_chars=""):
    if not add_chars or not isinstance(add_chars, str):
        add_chars=""

    if len_min is None:
        len_min = random.randint(2, 7)

    if len_max is None:
        len_max = random.randint(len_min, 10)

    _len = random.randint(len_min, len_max)
    _result = ""

    while len(_result) < _len:
        _result += random.choice(string.ascii_letters + add_chars)

    return _result

def generate_group_id():
    _groupId = [generate_str(2,4), generate_str(6, 10, add_chars="-").strip("-")]

    _comps = random.randint(3, 7)

    while len(_groupId) < _comps:
        _groupId.append(generate_str(3, random.randint(5, 12), add_chars="-").strip("-"))

    return ".".join(_groupId).lower()
    
def generate_gav(p=None, c=None):
    if c is True:
        c = generate_str(3, 5)

    # generate unique packaging if not given, but not reserved values
    if not p:
        while not p or p.lower() in ["zip", "txt", "sql", "plb"]:
            p = generate_str(3, 5)

    _gav = [generate_group_id(), generate_str(8, 11), generate_version(4), p]

    if c:
        _gav.append(c)

    return ':'.join(_gav).lower()

def generate_binary_file(len_min=None, len_max=None):
     _tf = tempfile.NamedTemporaryFile()
     _tf.seek(0, os.SEEK_SET)
     _tf.write(generate_bytes(len_min, len_max))
     return _tf

def generate_bytes(len_min=None, len_max=None):
    if len_min is None:
        len_min = random.randint(0, 77)

    if len_max is None:
        len_max = random.randint(len_min, 99)

    _len = random.randint(len_min, len_max)
    return bytes(random.getrandbits(8) for _t in range(0, _len))

def generate_zip_archive(include):
    _tf = tempfile.NamedTemporaryFile(suffix=".zip")

    with zipfile.ZipFile(_tf, mode='w') as _zip:
        for _f, _v in include.items():
            _v.get("file").flush()
            _v.get("file").seek(0, os.SEEK_SET)
            _zip.write(_v.get("file").name, _f)

    return _tf

def generate_many_different_gavs(amnt, p=None, c=None, current=None, duplicates=None):
    if not current:
        current = list()
    else:
        # we do not need to modify 'current' list but have to return its copy
        current = list(current)

    if not duplicates:
        duplicates = list()

    _clen = len(current)

    while len(current) < _clen + amnt:
        _gav = generate_gav(p=p, c=c)

        if any([_gav in current, _gav in duplicates]):
            continue

        current.append(_gav)

    return current
