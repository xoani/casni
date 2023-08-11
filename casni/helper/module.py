import types

__ignores__ = ['re', 'os', 'sys', 'types', 'threading']

def module_parser(objects, class_only: bool = False):
    # parse module contents
    _functions = []
    _classes = []
    _modules = []

    for k, v in dict(objects).items():
        if isinstance(v, types.FunctionType):
            if v != 'module_parser':
                _functions.append(k)
        if isinstance(v, type):
            _classes.append((k, v.__module__.split('.')[-1]))
        # filter the modules not want to expose
        if isinstance(v, types.ModuleType) and k not in __ignores__:
            _modules.append(k)
        del k, v

    # filter classes
    _classes = [c for c, m in _classes if m in _modules]
    if class_only:
        return _classes
    else:
        return _functions + _classes + _modules