import tempfile
import os
import shutil
import hashlib
from oc_sql_helpers.normalizer import PLSQLNormalizer, PLSQLNormalizationFlags

class MockPLSQLWrapper():
    def __init__(self):
        self.__sql_stubs_pth = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql-stubs")
        self.__sql_stubs_plain_pth = os.path.join(self.__sql_stubs_pth, "plain")
        self.__sql_stubs_wrapped_pth = os.path.join(self.__sql_stubs_pth, "wrapped")
        self.__normalization_flags = [
                PLSQLNormalizationFlags.no_comments,
                PLSQLNormalizationFlags.no_spaces,
                PLSQLNormalizationFlags.uppercase]
        self.__init_samples()

    def __init_samples(self):
        self.__samples = dict()

        for _dir in [self.__sql_stubs_plain_pth, self.__sql_stubs_wrapped_pth]:
            for _pth in os.listdir(_dir):
                _fn, _ext = os.path.splitext(_pth)
    
                if not _ext:
                    continue
    
                _tf = tempfile.NamedTemporaryFile()
    
                with open(os.path.join(_dir, _pth), mode='rb') as _fx:
                    PLSQLNormalizer().normalize(_fx, flags=self.__normalization_flags, write_to=_tf)
    
                _tf.flush()
                _tf.seek(0, os.SEEK_SET)
                _md5 = hashlib.md5(_tf.read()).hexdigest()
                _tf.close()
    
                _pth = os.path.join(self.__sql_stubs_wrapped_pth, "%s.plb" % _fn)
    
                if not os.path.exists(_pth):
                    self.__samples[_md5] = None
                    continue
    
                self.__samples[_md5] = _pth

    def __find_sample(self, fl_o):
        _seek = fl_o.tell()
        fl_o.seek(0, os.SEEK_SET)
        _tf = tempfile.NamedTemporaryFile()
        PLSQLNormalizer().normalize(fl_o, flags=self.__normalization_flags, write_to=_tf)
        _tf.flush()
        _tf.seek(0, os.SEEK_SET)
        _md5 = hashlib.md5(_tf.read()).hexdigest()
        _tf.close()
        fl_o.seek(_seek, os.SEEK_SET)
        
        if not self.__samples.get(_md5):
            return None

        return open(self.__samples.get(_md5), mode='rb')

    def wrap_buf(self, fl_o, write_to):
        if not write_to:
            raise ValueError("'write_to' required")

        _repr = self.__find_sample(fl_o)
        
        if not _repr:
            raise FileNotFoundError("Unable to wrap %s" % (fl_o.name if hasattr(fl_o, "name") else "<UNKNOWN>"))

        # calculate checksum and see the correspondent reference part
        write_to.seek(0, os.SEEK_SET)
        _repr.seek(0, os.SEEK_SET)
        shutil.copyfileobj(_repr, write_to)
        write_to.flush()
        _repr.close()

def mock_sql_wrap(fl_o, step):
    if not step:
        step = ""

    _result = dict()
    _tf = tempfile.NamedTemporaryFile()

    try:
        _wrapper = MockPLSQLWrapper()
        _wrapper.wrap_buf(fl_o, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        _result[step] = _tf

    except Exception as _e:
        _tf.close()
        _result = dict()
        pass

    return _result

