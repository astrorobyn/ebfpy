import numpy as np
import ebf

def ebf_iterate(fname, csize, start=None):
	"""
	replacement for iterate function that fails for large files
        fname: name of ebf file
        csize: size of one chunk in iteration
        start: row to start with
        
        returns an iterator over dictionaries containing csize rows of all the keys in fname
        WARNING: presumes file fname exists.
	"""

        key_list = ebf.keys(fname,'/')
	header=ebf.getHeader(fname[0],'/'+key_list[0])
        if start is None:
                begin=0
        else:
                begin=start
	end=min(start+csize, header.dim[0])

	while (begin<header.dim[0]):
		data = {}
		r = range(begin,end)

                for k in key_list:
                        data[k] = ebf.read_ind(fname[0],'/'+k, r)
                yield data

		begin = end
		end += csize
		if end>header.dim[0]: end = header.dim[0]


def ebf_iterate_tag(fname, tagname, csize, start=0):
	"""
	replacement for iterate function that fails for large files
	same as above, but for one column (called tagname)
	"""
        key_list = ebf.keys(fname,'/')

        if tagname not in key_list:
                raise RuntimeError('Error in ebf_iterate_tag: tagname {0} not found in file {1}'.format(tagname, fname))


	header=ebf.getHeader(fname[0],tagname)
	begin=start
	end=start+csize

	while (begin<header.dim[0]):
		r = range(begin,end)
                yield ebf.read_ind(fname[0],tagname, r)
		
                begin = end
		end += csize
		if end>header.dim[0]: end = header.dim[0]








