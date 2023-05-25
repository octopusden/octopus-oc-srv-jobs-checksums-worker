from ..generators.generators import generate_binary_file, generate_zip_archive
import os
from oc_cdtapi.NexusAPI import parse_gav, gav_to_path, NexusAPIError
import shutil
import hashlib

class MvnClientMock():
    __gavs=dict()

    def __init__(self):
        self.clear()
        self.__sql_stubs_pth = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql-stubs")

    def __check_open_sql_gav(self, gav):
        if gav in self.__gavs.keys():
            # nothing to do if this gav is known alreade
            return

        _gav = parse_gav(gav)

        # check we have "our" gav (sql, plb)
        if not _gav.get("p"):
            return

        if _gav["p"].lower() not in ["sql", "plb"]:
            return

        # "groupId" and "version" can be any - for easy creating "duplicates"

        _file_pth = os.path.join(self.__sql_stubs_pth,
                {"sql": "plain", "plb": "wrapped"}.get(_gav["p"].lower()), 
                "%s.%s" % tuple(map(lambda x: _gav[x].lower(), ["a", "p"])))

        if not os.path.exists(_file_pth):
            # no such artifact!
            return

        _fl = open(_file_pth, mode='rb')
        _fl.seek(0, os.SEEK_SET)
        self.__gavs[gav] = {
                "info": {"md5": self.md5(_fl), "mime": "text/plain"},
                "file": _fl}
        

    def clear(self):
        for _k,_v in self.__gavs.items():
            _f = _v.get("file")

            if not _f:
                continue

            if _f.closed:
                continue

            _f.close()

        self.__gavs = dict()

    def exists(self, gav):
        self.__check_open_sql_gav(gav)
        return bool(self.__gavs.get(gav))

    def set_gav_content(self, gav, tf, include=None):
        # NOTE: this may be used for overwriting "SQL" gavs with trash also
        _md5 = None

        if self.__gavs.get(gav):
            _f = self.__gavs.get(gav).get("file")
            _md5 = self.__gavs.get(gav)["info"]["md5"]

            if _f and not _f.closed:
                _f.close()

            if not tf:
                del(self.__gavs[gav])
                return

        self.__gavs[gav] = {
                "info": {"md5": self.md5(tf), "mime": "binary/data"},
                "file": tf}

        if include:
            self.__gavs[gav]["info"]["include"] = include

        _md5_n = self.__gavs[gav]["info"]["md5"]

        _other_md5 = list(filter(lambda x: x != gav, self.__gavs.keys()))
        _other_md5 = list(map(lambda x: self.__gavs.get(x).get("info").get("md5"), _other_md5))
        return all([_md5_n not in _other_md5, _md5 != _md5_n])

    def create_gav(self, gav, include=None):
        _gav = parse_gav(gav)

        if not _gav.get("p"):
            _gav["p"] = "jar"

        if _gav["p"].lower() not in ["zip", "sql", "plb"]:
            while not self.set_gav_content(gav, generate_binary_file()):
                continue

            return

        if _gav["p"].lower() in ["sql", "plb"]:
            self.__check_open_sql_gav(gav)

            if gav not in self.__gavs:
                raise FileNotFoundError(gav)

            return

        # here is ZIP
        if not include:
            # empty zip archive?
            include=list()

        for _c in include:
            if _c not in self.__gavs.keys():
                self.create_gav(_c)

        include = dict({gav_to_path(_x): self.__gavs.get(_x) for _x in  include})
        self.set_gav_content(gav, generate_zip_archive(include), include=include)

    def md5(self, tf):
        tf.seek(0, os.SEEK_SET)
        _hmd5 = hashlib.md5()

        while True:
            _chunk = tf.read(1 * 1024 * 1024)

            if not _chunk:
                break

            _hmd5.update(_chunk)

        return _hmd5.hexdigest()

    def info(self, gav):
        self.__check_open_sql_gav(gav)

        if gav not in self.__gavs.keys():
            raise NexusAPIError(code=404, text="Not found: %s" % gav)

        return self.__gavs.get(gav).get("info")

    def cat(self, gav, write_to=None, binary=True, stream=True):
        self.__check_open_sql_gav(gav)

        if gav not in self.__gavs.keys():
            raise NexusAPIError(code=404, text="Not found: %s" % gav)

        _tf = self.__gavs.get(gav).get("file")
        _tf.flush()
        _tf.seek(0, os.SEEK_SET)

        if not write_to:
            return _tf.read()

        write_to.seek(0, os.SEEK_SET)
        shutil.copyfileobj(_tf, write_to)
        write_to.flush()
