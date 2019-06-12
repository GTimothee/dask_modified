import math

import numpy as np 

from .. import config


__all__ = ("IO_optimizer", "Run", "Loading")

one_gig = 1000000000

class IO_optimizer():
    def __init__(self, mode='fixed_buffer_size', value=one_gig):
        """
        triggering_tasks: list of array on which is called ".compute()" in order of the program 
        io_tasks: set of IO loadings in order of the program
        arrays: dict of all arrays instances, key:array_name

        formats:
            IO_list: (dask_array_name, [(arr_name, slice), (arr_name, slice)...])
            run_list: (dask_array_name, dependencies)
            
        (dependencies: other dask array names)
        """
        self.is_optimization_enabled = True  
        self.compute_mode = False
        self.triggering_tasks = list()  
        self.generative_tasks = dict()
        self.io_tasks = dict()  
        self.arrays = dict()  

        self.init_buffer_size(mode, value)

    # setters

    def init_buffer_size(self, mode, value):
        if mode == 'fixed_buffer_size':
            self.BUFFER_SIZE = value
            self.buffer_size_available = value
        else:
            raise ValueError("not supported yet")

    def disable_opti(self):
        self.is_optimization_enabled = False

    def enable_opti(self):
        self.is_optimization_enabled = True

    def set_buffer_size_available(self, value):
        self.buffer_size_available = value

    def add_to_triggering_tasks(self, array):
        deps = get_dependencies_names(array)
        self.triggering_tasks.append((array.name, array, deps))

    def add_to_arrays(self, name, array):
        if name not in list(self.arrays.keys()):
            self.arrays[name] = array

    def add_to_io_tasks(self, array):
        if array.name not in list(self.io_tasks.keys()):
            self.io_tasks[array.name] = array

    def add_to_generative_tasks(self):
        # TODO
        pass

    # getters

    def is_enabled(self):
        return self.is_optimization_enabled

    def get_arrays(self):
        return self.arrays

    def get_triggering_tasks(self):
        return self.triggering_tasks

    def get_io_tasks(self):
        return self.io_tasks

    def get_buffer_size(self):
        return self.BUFFER_SIZE

    def get_buffer_size_available(self):
        return self.buffer_size_available
    
    def get_generative_tasks(self):
        # TODO
        pass

    # methods

    def compute_next_run(self):
        """ function for step by step execution
        """
        return

    def compute(self):
        self.compute_mode = True
        task_list = self.triggering_tasks.copy()
        result = None
        while len(task_list) > 0:
            run = self.get_next_run(task_list)
            if run.contains_optimizations():
                run.preload_data(self.get_arrays())
            result = run.compute(self.get_triggering_tasks())
        self.compute_mode = False
        return result

    def get_next_run(self, task_list):
        run = Run()
        while len(task_list) > 0:
            next_task = task_list.pop(0)
            print("next task", next_task)
            loading = self.process_triggering_task(next_task)
            if run.is_empty():
                print("empty, adding task")
                memory_cost = run.add(next_task, loading, self.get_arrays())
                print("memory_cost", memory_cost)
                self.set_buffer_size_available(self.get_buffer_size_available() - memory_cost)
            else:
                print("testing combination")
                if run.compatible(loading, self.get_arrays()):
                    print("combination possible, merging")
                    memory_cost = run.merge(next_task, loading, self.get_arrays())
                    self.set_buffer_size_available(self.get_buffer_size_available() - memory_cost)
                else:
                    print("combination not posible, returning current run")
                    return run
        print("no more triggering tasks, returning result")
        return run

    def process_triggering_task(self, task):
        """ dependencies: list of names of dependency arrays
        getitem -> array -> array-original
        """
        dependencies = task[2]
        print("dependencies", dependencies)
        io_deps = list()
        for dependency_name in dependencies: 
            if self.generative_tasks and dependency_name in self.generative_tasks:
                #TODO: reduce buffer size available
                pass
            if self.io_tasks and dependency_name in list(self.io_tasks.keys()):
                dask_array = self.io_tasks[dependency_name]
                dep = get_loadings_from_dask_array(dask_array)
                io_deps.append(dep)
        return self.process_io_dependencies(io_deps)

    def process_io_dependencies(self, io_deps):
        print("io_deps", io_deps)
        arrays_dict = self.group_slices_by_array(io_deps)
        print("arrays_dict", arrays_dict)
        loading = Loading(arrays_dict, self.get_arrays())
        if loading.can_be_optimized():
            if loading.get_memory_print(self.get_arrays()) <= self.buffer_size_available: 
                loading.set_optimization()
        return loading

    def group_slices_by_array(self, io_deps):
        d = dict()
        for dep in io_deps:
            target_array_name = dep[1]
            slices_list = dep[2]  
            if target_array_name in list(d.keys()):
                d[target_array_name] += slices_list
            else:
                d[target_array_name] = slices_list
        return d

    def in_compute_mode(self):
        return self.compute_mode

   
class Loading():
    def __init__(self, arrays_dict, optimizer_arrays_list):
        self._is_possible = False
        self._is_enabled = False
        self.array_loadings = None # values: list of lists of contiguous slices 
        self.optimized_array_loadings = None # values: list of lists of contiguous slices
        self.test_if_possible(arrays_dict, optimizer_arrays_list)

    # getters

    def get_loadings(self):
        if self.is_optimization_enabled():
            return self.optimized_array_loadings
        return self.array_loadings

    def can_be_optimized(self):
        return self._is_possible

    def is_optimization_enabled(self):
        return self._is_enabled

    # setters

    def set_optimization(self):
        if not self.can_be_optimized():
            raise ValueError("cannot enable optimization if optimization is not possible")
        self._is_enabled = True

    def test_if_possible(self, arrays_dict, optimizer_arrays_list):
        """ Test if optimization is possible for any array in arrays_dict. 
        for a given array, merge slices if possible.
        By the way create array_loadings from array_dicts
        Example: 
            Before: 
                arrays_dict = {array_1: [1,2,5,6,7], array2: [3,8,9]}
            After:
                arrays_dict = {array_1: [[1,2], [5,6,7]], array2: [[3], [8,9]]}
        """
        array_loadings = dict()
        optimized_array_loadings = dict()

        for array_name in list(arrays_dict.keys()):
            slices = arrays_dict[array_name]

            chunk_shape = optimizer_arrays_list[array_name].chunks            
            shape = tuple([len(shape_tuple) for shape_tuple in chunk_shape])
            chunk_shape = tuple([shape_tuple[0] for shape_tuple in chunk_shape])
            
            print("shape", shape)
            numeric_slices = sorted(list(map(lambda x: _3d_to_numeric_pos(x, shape, order='C'), slices)))
            print("numeric_slices", numeric_slices)

            load_possibility = self.find_optimization_possibility(numeric_slices)
            print("load_possibility", load_possibility)
            
            if load_possibility and self.better_than_not_optimized():
                self._is_possible = True 
                optimized_array_loadings[array_name] = load_possibility
            else:
                array_loadings[array_name] = [[s for s in numeric_slices]]
                optimized_array_loadings[array_name] = array_loadings[array_name]
                
        if not self._is_possible:
            print("cannot be optimized")
        else:
            print("can be optimized")

        self.array_loadings = array_loadings
        self.optimized_array_loadings = optimized_array_loadings

    # methods

    def better_than_not_optimized(self):
        """ return true for the moment because we just load contiguous parts so no fancy optimizations to verify
        """
        #TODO support more cases
        return True

    def find_optimization_possibility(self, slices_numeric_list):
        """ For the moment if slices are contiguous we optimize it by merging slices into one loading
        """
        #TODO support more cases
        test_list = slices_numeric_list.copy()

        if len(test_list) == 1:
            return None

        previous = test_list.pop(0)
        while len(test_list) > 0:
            next_slice = test_list.pop(0)
            if next_slice != previous + 1:
                return None
            previous = next_slice
        return [slices_numeric_list]

    def get_memory_print(self, optimizer_arrays_list):
        """ For the moment we only support the case where we load one block entirely at a time (not parts of a block).
        A block is a logical block here: the logical chunk shape of dask array.
        """
        # TODO: support more cases
        _sum = 0
        size_of_voxel_in_bytes = 2 # TODO modify it to make it generalizable
        loadings = self.array_loadings
        if self.is_optimization_enabled():
            loadings = self.optimized_array_loadings
        for array_name, _list in loadings.items():
            local_sum = 0
            array_obj = optimizer_arrays_list[array_name]
            shape = [chunk_shape[0] for chunk_shape in array_obj.chunks]
            block_size_in_bytes = shape[0] * shape[1] * shape[2] * size_of_voxel_in_bytes
            for contiguous_load in _list:
                local_sum += len(contiguous_load)  # len(contiguous_load): number of blocks to be loaded
            _sum += local_sum * block_size_in_bytes
        return _sum


class Run():
    def __init__(self):
        self._contains_optimizations = False
        self.tasks = list()
        self.loadings = dict()

    def is_empty(self):
        if len(self.tasks) == 0:
            return True
        return False

    def contains_optimizations(self):
        return self._contains_optimizations

    def add(self, task, loading, optimizer_arrays_list): 
        if not task:
            raise ValueError("no task to add")
        self.tasks.append(task)
        if loading.is_optimization_enabled():  # merge array_dict from optimization with the one in run
            self._contains_optimizations = True
        self.loadings = loading.get_loadings()
        return self.get_memory_print(optimizer_arrays_list, self.loadings)

    def merge(self, task, merged_loadings, optimizer_arrays_list):
        """ return the added memory cost of merging
        """
        init_memory_print = self.get_memory_print(optimizer_arrays_list, self.loadings)
        if not task:
            raise ValueError("no task to add")
        self.tasks.append(task)
        if not self.contains_optimizations():
            self._contains_optimizations = True
        self.loadings = merged_loadings
        result_memory_print = self.get_memory_print(optimizer_arrays_list, self.loadings)
        return result_memory_print - init_memory_print

    def get_memory_print(self, arrays, loadings):
        """ For the moment we only support the case where we load one block entirely at a time (not parts of a block).
        A block is a logical block here: the logical chunk shape of dask array.
        loadings: a list_of_lists
        """
        # TODO: support more cases
        # if more optimizations are made available, then combining the optimization in parameter with self.arrays_dict can change memory print
        # therefore in the above case it should be recomputed
        
        _sum = 0
        size_of_voxel_in_bytes = 2 # TODO modify it to make it generalizable
        for array_name, loads in loadings.items():
            local_sum = 0
            array_obj = arrays[array_name]
            shape = [chunk_shape[0] for chunk_shape in array_obj.chunks]
            block_size_in_bytes = shape[0] * shape[1] * shape[2] * size_of_voxel_in_bytes
            for load in loads:
                local_sum += len(load)  # len(load): number of blocks to be loaded
            _sum += local_sum * block_size_in_bytes
        return _sum

    def compatible(self, candidate_loading, optimizer_arrays_list):
        """ if loads are overlaping AND enough memory available
        """
        merged_loadings = self.loadings.copy()
        if not merged_loadings or not candidate_loading:
            raise ValueError("cannot merge if one loadings dict is missing")

        optimizations_occured = False
        for name, list_of_contiguous_loads in candidate_loading:
            if not name in list(merged_loadings.keys()):
                merged_loadings[name] = list_of_contiguous_loads
            else:
                current_loads = merged_loadings[name]
                all_blocks = flatten_list_of_lists(current_loads) + flatten_list_of_lists(list_of_contiguous_loads)
                all_blocks = sorted(remove_duplicates(all_blocks))
                result = self.optimize(all_blocks)
                merged_loadings[name] = result
                optimizations_occured = True
        
        if optimizations_occured:
            if self.merging_wanted():
                return merged_loadings, self.get_memory_print(optimizer_arrays_list, self.loadings)
        return None, None

    def merging_wanted(self):
        return True

    def optimize(self, all_blocks):
        """ concatenate contiguous block indices into lists
        return a list of lists
        """
        result = list()
        curr = list()
        for block_index in all_blocks:
            if len(curr) == 0:
                curr.append(block_index)
            elif curr[-1] == block_index - 1:
                curr.append(block_index)
            else:
                result.append(curr.copy())
                curr = list()
        return result

    def preload_data(self, optimizer_arrays_list):
        """ buffer dict -> each key is a orig_array_name, each value is a dict containing loaded arrays
        for the moment it only supports the case where a loading is an entire block
        """
        def verify_array_key(array_name, dask_graph):
            k = list(dask_graph.keys())[0]  # only key = name of the array
            print("array_name", k)
            if array_name != k:
                raise ValueError("should be the same", array_name, k)

        # TODO: support other cases
        buffer_dict = dict() # dict of arrays 
        for array_name, loading_list in self.loadings.items():
            print("array_name", array_name)
            dask_array = optimizer_arrays_list[array_name]
            chunk_shape = dask_array.chunks            
            blocks_shape = tuple([len(shape_tuple) for shape_tuple in chunk_shape])
            chunk_shape = tuple([shape_tuple[0] for shape_tuple in chunk_shape])

            dask_graph = dask_array.dask.dicts
            verify_array_key(array_name, dask_graph)

            original_array_key = get_original_array_key(dask_graph[array_name])
            hdf5_original_array = dask_graph[array_name][original_array_key]
            print(hdf5_original_array)
            for l in loading_list:
                if len(l) > 1:  # if there is optimizations on it
                    _slices = convert_list_of_num_slices_to_numpy_slices(l, blocks_shape, chunk_shape)
                    if not array_name in (buffer_dict.keys()):
                        buffer_dict[array_name] = dict()
                    for numeric_block_pos in l:
                        _3d_block_pos = numeric_to_3d_pos(numeric_block_pos, blocks_shape, order='C')
                        buffer_dict[array_name][_3d_block_pos] = hdf5_original_array[_slices]

        self.modify_getitem_tasks(optimizer_arrays_list, buffer_dict)  # for each getitem in each run_task to run, if array_proxy is in buffer_dict, then modify getitemtaks
        return

    def modify_getitem_tasks(self, optimizer_arrays_list, buffer_dict):
        for task in self.tasks:
            _, task_dask_array, io_deps_names = task
            for dep in io_deps_names:
                getitem_dict = task_dask_array.dask.dicts[dep]
                the_only_key = list(getitem_dict.keys())[0]

                #print("before: ", getitem_dict[the_only_key])

                task_entry = list(getitem_dict[the_only_key])

                print("before: ", getitem_dict[the_only_key])

                print("task entry", task_entry)
                array_key = task_entry[1]
                print("array_key", array_key)
                l = list(array_key)
                array_proxy_name = l[0]
                _slice = tuple(l[1:]) 
                buffer_content = buffer_dict[array_proxy_name][_slice]
                task_entry[1] = buffer_content
                getitem_dict[the_only_key] = tuple(task_entry)

                print("after: ", getitem_dict[the_only_key])

    def compute(self, optimizer_trigger_arrays_list):
        for task in self.tasks:
            dask_array = task[1]
            a = dask_array.compute()
        return a

# utility functions

def convert_list_of_num_slices_to_numpy_slices(l, blocks_shape, chunk_shape):
    first, last = (l[0], l[-1])
    first_block_3d_pos, last_block_3d_pos = (numeric_to_3d_pos(first, blocks_shape, order='C'), 
                                             numeric_to_3d_pos(last, blocks_shape, order='C'))
    f = _3d_pos_of_first_pix = [first_block_3d_pos[i] * chunk_shape[i] for i in range(len(first_block_3d_pos))]
    l = _3d_pos_of_last_pix = [(last_block_3d_pos[i] + 1) * chunk_shape[i] for i in range(len(last_block_3d_pos))]
    _slice = np.s_[f[0]:l[0],f[1]:l[1],f[2]:l[2]]
    return _slice

def numeric_to_3d_pos(numeric_pos, shape, order):
    if order == 'C':  
        nb_blocks_per_row = shape[0]
        nb_blocks_per_slice = shape[0] * shape[1]
    elif order == 'F':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    
    i = math.floor(numeric_pos/nb_blocks_per_slice)
    numeric_pos -= i * nb_blocks_per_slice
    j = math.floor(numeric_pos/nb_blocks_per_row)
    numeric_pos -= j * nb_blocks_per_row
    k = numeric_pos
    return (i, j, k)

def _3d_to_numeric_pos(_3d_pos, shape, order):
    """
    in C order for example, should be ((_3d_pos[0]-1) * nb_blocks_per_slice) 
    but as we start at 0 we can keep (_3d_pos[0] * nb_blocks_per_slice)
    """
    if order == 'C':  
        nb_blocks_per_row = shape[0]
        nb_blocks_per_slice = shape[0] * shape[1]
    elif order == 'F':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    return (_3d_pos[0] * nb_blocks_per_slice) + (_3d_pos[1] * nb_blocks_per_row) + _3d_pos[2] 

def get_original_array_key(array_dict):
    """ array_dict: array graph
    """
    original_arr_key = None
    for key in list(array_dict.keys()):
        if isinstance(key, str): 
            print(key)
            split = key.split('-')
            if split[0] == "array" and split[1] == "original":
                original_arr_key = key
                break
    return original_arr_key

def remove_duplicates(_list):
    return list(set(_list)) 

def flatten_list_of_lists(l_of_lists):
    l = list()
    for _l in l_of_lists:
        l = l + _l
    return l

def get_dependencies_names(dask_array):
        """ Return a list of IO dependencies.
        """
        l = list()
        for key in list(dask_array.dask.dicts.keys()):
            if isinstance(key, str) and key.split('-')[0] == 'getitem':
                l.append(key)
        return l

def get_slice_from_getitem_dict(getitem_dict):
    data = list(getitem_dict.items())[0][1][1]
    target_array_name = data[0]
    _slice = data[1:]
    return target_array_name, tuple(_slice) 

def get_loadings_from_dask_array(dask_array):
    """ we assume all getitem are from same original-array
    """
    slices = list()
    io_array_name = dask_array.name 
    dask_graph = dask_array.dask.dicts
    for key in list(dask_graph.keys()):
        _type = key.split('-')[0]
        if "getitem" == _type:
            getitem_dict = dask_graph[key]
            array_key, _slice = get_slice_from_getitem_dict(getitem_dict)
            slices.append(_slice)
    return (io_array_name, array_key, slices)

    