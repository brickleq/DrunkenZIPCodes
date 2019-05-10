#%%
# Dependencies
import numpy as np
import pandas as pd
#%%




'''
dict = scipy.io.readsav('Resources/pew_data_2014.sav')

DataFrame.append(other, ignore_index=False, verify_integrity=False, sort=None)[source]
Append rows of other to the end of caller, returning a new object.

Columns in other that are not in the caller are added as new columns.

Parameters:	
other : DataFrame or Series/dict-like object, or list of these
The data to append.

ignore_index : boolean, default False
If True, do not use the index labels.

verify_integrity : boolean, default False
If True, raise ValueError on creating index with duplicates.

sort : boolean, default None
Sort columns if the columns of self and other are not aligned. The default sorting is deprecated and will change to not-sorting in a future version of pandas. Explicitly pass sort=True to silence the warning and sort. Explicitly pass sort=False to silence the warning and not sort.

New in version 0.23.0.




#%%
    scipy.io.readsav
scipy.io.readsav(file_name)
Read an IDL .sav file

Parameters:	
file_name : str

Name of the IDL save file.

idict : dict, optional

Dictionary in which to insert .sav file variables

python_dict : bool, optional

By default, the object return is not a Python dictionary, but a case-insensitive dictionary with item, attribute, and call access to variables. To get a standard Python dictionary, set this option to True.

uncompressed_file_name : str, optional

This option only has an effect for .sav files written with the /compress option. If a file name is specified, compressed .sav files are uncompressed to this file. Otherwise, readsav will use the tempfile module to determine a temporary filename automatically, and will remove the temporary file upon successfully reading it in.

verbose : bool, optional

Whether to print out information about the save file, including the records read, and available variables.

Returns:	
idl_dict : AttrDict or dict

If python_dict is set to False (default), this function returns a case-insensitive dictionary with item, attribute, and call access to variables. If python_dict is set to True, this function returns a Python dictionary with all variable names in lowercase. If idict was specified, then variables are written to the dictionary specified, and the updated dictionary is returned.
'''


#%%
