#import pkgutil

#__all__ = []

#for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
#    __all__.append(module_name)
#    _module = loader.find_module(module_name).load_module(module_name)
#    globals()[module_name] = _module

#from .datanetcdf import DataNetcdf
#from .dataparameter import DataParameter
#from .dataarray import DataArray
#from .dataimage import DataImage, ImageGeotiff, ImageShape
#from .dataraw import DataRaw
#from .datadb import DataDb
#from .datahdfeos import DataHdfeos
