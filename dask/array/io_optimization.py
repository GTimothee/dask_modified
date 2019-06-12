from .. import config
import math
import numpy as np

import sys

def optimize_io(dsk, keys):
    dask_graph = dsk.dicts
    dask_graph = get_loadings_from_dask_graph(dask_graph)
    #print_array_parts(dask_graph)

    #sys.exit()
    return dsk


def get_loadings_from_dask_graph(dask_graph):
    """ we assume all getitem are from same original-array
    """

    def extract_slices_dict():

        def update_slices_dict(proxy_array_name, slices_list):
            if proxy_array_name in list(slices_dict.keys()):
                slices_dict[proxy_array_name] = slices_dict[proxy_array_name] + slices_list
            else:
                slices_dict[proxy_array_name] = slices_list

        def update_deps_dict(proxy_array_name, key):
            if proxy_array_name in deps_dict.keys():
                if key not in deps_dict[proxy_array_name]:
                    deps_dict[proxy_array_name].append(key)
            else:
                deps_dict[proxy_array_name] = [key]

        def update_dicts(proxy_array_name, slices_list, key):
            if not proxy_array_name in list(array_to_original.keys()):
                    update_dims_dict(dask_graph, proxy_array_name, array_to_original, dims_dict, original_array_shapes)
            array_block_dims = dims_dict[array_to_original[proxy_array_name]]
            slices_list = [_3d_to_numeric_pos(_slice, array_block_dims, order='C') for _slice in slices_list]
            update_slices_dict(proxy_array_name, slices_list)
            update_deps_dict(proxy_array_name, key)

        if config.get("io-optimizer.mode") == 'rechunked':
            rechunk_keys, rechunk_graph, _, _ = get_rechunk_keys_lists_from_dask_array(dask_graph, printer=False)
            for key, parent_name in rechunk_keys:
                proxy_array_name, slices_list = get_slices_from_rechunk_dict(rechunk_graph) 
                update_dicts(proxy_array_name, slices_list, parent_name)
        else:
            getitem_keys, _, _ = get_keys_lists_from_dask_array(dask_graph, printer=False)
            for key in getitem_keys:
                proxy_array_name, slices_list = get_slices_from_getitem_dict(dask_graph[key]) 
                update_dicts(proxy_array_name, slices_list, key)

    def merge_io_tasks():
        def create_root_node(dask_graph, proxy_array_name, load, img_nb_blocks_per_dim, img_chunks_sizes):
            """
            -take array name task - this will be our root node
            -for each slice, remove the slice task from array_name_task
            -create a new slice task which is the combined slice of the slices of the load
            -add it to the array_name global task

            better?
            -just add a new key called 'merged part...'  
            """
            def get_coords_in_image(block_coord, img_chunks_sizes):
                return tuple([block_coord[i] * img_chunks_sizes[i] for i in range(3)])

            def get_target_slice(load, img_nb_blocks_per_dim, img_chunks_sizes):
                """print("start", load[0])
                print("end", load[-1])"""

                _min = [100000000, 100000000, 100000000]
                _max = [0, 0, 0]
                for i in range(load[0], load[-1] + 1):
                    t = numeric_to_3d_pos(i, img_nb_blocks_per_dim, order='C') 
                    for j in range(3):
                        if t[j] > _max[j]:
                            _max[j] = t[j]
                        if t[j] < _min[j]:
                            _min[j] = t[j]

                start = tuple(_min)
                end = tuple(_max)

                """print("start", start)
                print("end", end)"""

                start = get_coords_in_image(start, img_chunks_sizes)
                end = tuple([x + 1 for x in end])
                end = get_coords_in_image(end, img_chunks_sizes)
                return (slice(start[0], end[0], None),
                        slice(start[1], end[1], None),
                        slice(start[2], end[2], None))
            
            array_proxy_dict = dask_graph[proxy_array_name]
            merged_array_proxy_name = 'merged-part-' + str(load[0]) + '-' + str(load[-1])
            key = (merged_array_proxy_name, 0, 0, 0)
            slice_tuple = get_target_slice(load, img_nb_blocks_per_dim, img_chunks_sizes)
            get_func = array_proxy_dict[list(array_proxy_dict.keys())[0]][0]
            original_array_name = array_proxy_dict[list(array_proxy_dict.keys())[0]][1]
            value = (get_func, original_array_name, (slice_tuple[0], slice_tuple[1], slice_tuple[2]))
            dask_graph[merged_array_proxy_name] = {key:value}
            return merged_array_proxy_name


        def update_io_tasks(proxy_array_name, 
                            proxy_arrays_to_task_names_dict, 
                            merged_task_name,
                            img_nb_blocks_per_dim,
                            img_chunks_sizes,
                            load):
            """
            -find the tasks using the array_name (using dependencies?)
            for each task
                -replace the array_name subpart 
                -replace the slices part
            """
            
            def replace_proxy_array_by_merged_proxy(rechunk_dict, proxy_array_name):
                """def replace_rechunk_merge(val):
                    _, concat_list = val
                    while not isinstance(concat_list[0][0], tuple):
                        concat_list = concat_list[0]
                    for _list in concat_list:
                        for i in range(len(_list)):
                            tupl = _list[i]
                            if proxy_array_name == tupl[0]:
                                #target_name, s1, s2, s3 = tupl
                                pass
                    return"""
                
                def replace_rechunk_split(val):
                    get_func, target_key, slices = val
                    _, s1, s2, s3 = target_key
                    proxy_array_part = (s1, s2, s3)
                    array_part_num = _3d_to_numeric_pos(proxy_array_part, img_nb_blocks_per_dim, order='C') 
                    if array_part_num in load:
                        slice_of_interest = get_slice_from_proxy_part_key(proxy_array_part, img_chunks_sizes, merged_task_name, img_nb_blocks_per_dim, slices)
                        new_val = (get_func, (merged_task_name, 0, 0, 0), slice_of_interest)
                        return new_val
                    else:
                        return val

                for k in list(rechunk_dict.keys()):
                    key_name = k[0]
                    val = rechunk_dict[k]
                    if 'rechunk-merge' in key_name:
                        #new_val = replace_rechunk_merge(val)
                        #rechunk_dict[k] = new_val
                        pass
                    elif 'rechunk-split' in key_name:
                        new_val = replace_rechunk_split(val)
                        rechunk_dict[k] = new_val
                    else:
                        pass 
            
            def get_tasks_targeting_proxy(getitem_dict, proxy_array_name):
                return [(k,v) for k, v in getitem_dict.items() if v[1][0] == proxy_array_name]

            def get_slice_from_proxy_part_key(proxy_array_part_targeted, img_chunks_sizes, merged_task_name, img_nb_blocks_per_dim, slices):
                """
                proxy_array_part_targeted: tuple (x, y, z) from (proxy_array_name, x, y, z)
                """
                _, _, start_of_block, _ = merged_task_name.split('-')

                # convert 3d pos in image to 3d pos in merged block
                num_pos = _3d_to_numeric_pos(proxy_array_part_targeted, img_nb_blocks_per_dim, order='C') # TODO à remove car on le fait deja avant
                num_pos_in_merged = num_pos - int(start_of_block)
                proxy_array_part_in_merged = numeric_to_3d_pos(num_pos_in_merged, img_nb_blocks_per_dim, order='C')

                _slice = proxy_array_part_in_merged

                start = [None] * 3
                stop = [None] * 3
                for i, sl in enumerate(slices):
                    if sl.start != None:
                        start[i] = (_slice[i] * img_chunks_sizes[i]) + sl.start
                    else:
                        start[i] = _slice[i] * img_chunks_sizes[i]

                    if sl.stop != None:
                        stop[i] = (_slice[i] * img_chunks_sizes[i]) + sl.stop
                    else:
                        stop[i] = (_slice[i] + 1) * img_chunks_sizes[i] 
                        
                return (slice(start[0], stop[0], None),
                        slice(start[1], stop[1], None),
                        slice(start[2], stop[2], None))


            def get_slice_from_merged_task_name(merged_task_name, target_slice, img_nb_blocks_per_dim):
                _, _, start_of_block, _ = merged_task_name.split('-')
                
                sot = [s.start for s in target_slice]
                sot = _3d_to_numeric_pos(sot, img_nb_blocks_per_dim, order='C')
                sot = sot - start_of_block

                eot = [s.stop + 1 for s in target_slice]
                eot = _3d_to_numeric_pos(eot, img_nb_blocks_per_dim, order='C')
                eot = eot - start_of_block

                return (slice(sot[0], eot[0], None), 
                        slice(sot[1], eot[1], None), 
                        slice(sot[2], eot[2], None))

            if config.get("io-optimizer.mode") == 'rechunked':
                rechunk_task_keys = proxy_arrays_to_task_names_dict[proxy_array_name]
                rechunk_dicts = [(rechunk_task_key, dask_graph[rechunk_task_key]) for rechunk_task_key in rechunk_task_keys]                       

                for rechunk_task_key, rechunk_dict in rechunk_dicts:
                    replace_proxy_array_by_merged_proxy(rechunk_dict, proxy_array_name)
            else:
                getitem_task_names = proxy_arrays_to_task_names_dict[proxy_array_name]
                # dict with key = proxy_array_name, val = tasks using this proxy_array
                getitem_dicts = [(getitem_task_name, dask_graph[getitem_task_name]) for getitem_task_name in getitem_task_names] 
                for task_name, getitem_dict in getitem_dicts:
                    print("processing", task_name, "for load", load)
                    subtasks = get_tasks_targeting_proxy(getitem_dict, proxy_array_name)
                    for kv_pair in subtasks: 
                        getitem_key, value = kv_pair
                        get_func, _, slices = value # TODO replace _ by proxy_key and replace value[1] below by proxy_key
                        slices_key = tuple(value[1][1:])
                        num_pos = _3d_to_numeric_pos(slices_key, img_nb_blocks_per_dim, order='C')
                        
                        if num_pos in load:
                            #print("\tbeing treated", value[1])
                            if np.all([tuple([sl.start, sl.stop]) == (None, None) for sl in slices]):
                                proxy_array_part_targeted = value[1][1:]
                                slice_of_interest = get_slice_from_proxy_part_key(proxy_array_part_targeted, 
                                                                                img_chunks_sizes, 
                                                                                merged_task_name, 
                                                                                img_nb_blocks_per_dim,
                                                                                None) # TODO a modifier
                            else:
                                raise ValueError("TODO!")
                                #slice_of_interest = get_slice_from_merged_task_name(merged_task_name, slices, img_nb_blocks_per_dim)
                            new_value = (get_func, (merged_task_name, 0, 0, 0), slice_of_interest)
                            getitem_dict[getitem_key] = new_value # replace
            return


        # core of the function merge_io_tasks
        for proxy_array_name, l in slices_dict.items(): # [[1,2], [3]]
            
            # get list of lists
            _, img_chunks_sizes = original_array_shapes[array_to_original[proxy_array_name]]
            img_nb_blocks_per_dim = dims_dict[array_to_original[proxy_array_name]]
            list_of_lists = merge_what_can_be_merged(l, img_chunks_sizes, img_nb_blocks_per_dim)
            print("list_of_lists", list_of_lists, '\n')
            # for each load
            for load_index in range(len(list_of_lists)): # [1,2]
                load = list_of_lists[load_index]
                if len(load) > 1:
                    merged_task_name = create_root_node(dask_graph, proxy_array_name, load, img_nb_blocks_per_dim, img_chunks_sizes)
                    update_io_tasks(proxy_array_name, deps_dict, merged_task_name, img_nb_blocks_per_dim, img_chunks_sizes, load)
        return dask_graph
                
    # core
    slices_dict = dict()
    array_to_original = dict() # map array to original-array
    dims_dict = dict()  # map original-array-name with shape needed for 3d_to_num_pos
    original_array_shapes = dict()
    deps_dict = dict() # map getitem task to array_key from which it does the getitem
    
    extract_slices_dict()
    return merge_io_tasks()


def merge_what_can_be_merged(l, block_shape, img_nb_blocks_per_dim, nb_bytes_per_val=8):
    """ current strategy : entire blocks, TODO support more strategies
    """
    def new_list(list_of_lists, prev_i):
        list_of_lists.append(list())
        prev_i = None

    def get_load_strategy(buffer_mem_size):
        block_mem_size = block_shape[0] * block_shape[1] * block_shape[2] * nb_bytes_per_val
        block_row_size = block_mem_size * img_nb_blocks_per_dim[2]
        block_slice_size = block_row_size * img_nb_blocks_per_dim[1]

        if buffer_mem_size >= block_slice_size:
            nb_slices = math.floor(buffer_mem_size / block_slice_size)
            return "slices", nb_slices * img_nb_blocks_per_dim[2] * img_nb_blocks_per_dim[1]
        elif buffer_mem_size >= block_row_size:
            nb_rows = math.floor(buffer_mem_size / block_row_size)
            return "rows", nb_rows * img_nb_blocks_per_dim[2]
        else:
            return "blocks", math.floor(buffer_mem_size / block_mem_size)

    def test_if_create_new_load(blocks_dont_follow, max_nb_of_blocks_reached):
        def bad_configuration_incoming():
            """ to avoid bad configurations in clustered writes
            """
            if not prev_i:
                return False 

            # do the test
            if strategy == "blocks" and prev_i % img_nb_blocks_per_dim[2] == 0:
                return True 
            elif strategy == "rows" and prev_i % (img_nb_blocks_per_dim[1] * img_nb_blocks_per_dim[1]) == 0:
                return True 
            else:
                return False

        len_of_current_list = len(list_of_lists[len(list_of_lists) - 1])

        if len_of_current_list == nb_blocks_per_load:
            max_nb_of_blocks_reached = True
        if prev_i and next_i != prev_i + 1:
            blocks_dont_follow = True
        if max_nb_of_blocks_reached or blocks_dont_follow or bad_configuration_incoming():
            new_list(list_of_lists, prev_i)

    def add_to_current_load(list_of_lists, next_i):
        list_of_lists[len(list_of_lists) - 1].append(next_i)

    if not config.get("io-optimizer"):
        raise ValueError("io-optimizer not enabled")

    buffer_mem_size = config.get("io-optimizer.memory_available")
    strategy, nb_blocks_per_load = get_load_strategy(buffer_mem_size)

    list_of_lists = list()
    prev_i = None
    new_list(list_of_lists, prev_i)
    blocks_dont_follow = False
    max_nb_of_blocks_reached = False

    l = list(set(l))
    l = sorted(l)
    while len(l) > 0:
        next_i = l.pop(0)
        test_if_create_new_load(blocks_dont_follow, max_nb_of_blocks_reached)
        add_to_current_load(list_of_lists, next_i)
        
        prev_i = next_i       
        max_nb_of_blocks_reached = False 
        blocks_dont_follow = False
    return list_of_lists


def update_dims_dict(dask_graph, array_key, deps_dict, dims_dict, original_array_shapes):
    """
    dask graph: 
    array_key: key in the dask graph corresponding to an array proxy using the original array 

    modified by this function :
        deps_dict: key=array proxy, value=original array used by the array proxy
        original_array_shapes: key=original array name, value = (shape, chunks)
        dims_dict: key=original array name, value = nb blocks in each dimensions
    """
    def get_original_array_name_from_array_proxy_key(dask_graph, array_key):
        for chunk_key in list(dask_graph[array_key].keys()):
            if isinstance(chunk_key, str):
                if 'array-original' in chunk_key:
                    return chunk_key

    original_array_name = get_original_array_name_from_array_proxy_key(dask_graph, array_key)
    original_array = dask_graph[array_key][original_array_name]
    dimensions = (original_array.shape, original_array.chunks)

    # update dicts
    deps_dict[array_key] = original_array_name
    original_array_shapes[original_array_name] = dimensions
    dims_dict[original_array_name] = get_array_block_dims(dimensions[0], dimensions[1])


def get_array_block_dims(shape, chunks):
    """ from shape of image and size of chukns=blocks, return the dimensions of the array in terms of blocks
    i.e. number of blocks in each dimension
    """
    if not len(shape) == len(chunks):
        raise ValueError("chunks and shape should have the same dimension", shape, chunks)
    return [int(s/c) for s, c in zip(shape, chunks)]


def numeric_to_3d_pos(numeric_pos, shape, order):
    if order == 'F':  
        # nb_blocks_per_row = shape[0]
        # nb_blocks_per_slice = shape[0] * shape[1]
        raise ValueError("to_verify")
    elif order == 'C':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    
    i = math.floor(numeric_pos / nb_blocks_per_slice)
    numeric_pos -= i * nb_blocks_per_slice
    j = math.floor(numeric_pos / nb_blocks_per_row)
    numeric_pos -= j * nb_blocks_per_row
    k = numeric_pos
    return (i, j, k)

def _3d_to_numeric_pos(_3d_pos, shape, order):
    """
    in C order for example, should be ((_3d_pos[0]-1) * nb_blocks_per_slice) 
    but as we start at 0 we can keep (_3d_pos[0] * nb_blocks_per_slice)
    """
    if order == 'F':  
        # nb_blocks_per_row = shape[0]s
        # nb_blocks_per_slice = shape[0] * shape[1]
        raise ValueError("to_verify")
    elif order == 'C':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    return (_3d_pos[0] * nb_blocks_per_slice) + (_3d_pos[1] * nb_blocks_per_row) + _3d_pos[2] 


def get_slices_from_rechunk_dict(rechunk_merge_graph):
    """
    -assumes that the rechunk merge dict only take data from a same proxy array
    """
    slices_list = list()
    rechunk_merges = list()
    rechunk_splits = list()
    for key in list(rechunk_merge_graph.keys()):
        if 'rechunk-merge' in key[0]:
            rechunk_merges.append(key)
        elif 'rechunk-split' in key[0]:
            rechunk_splits.append(key)
        else:
            pass 
    
    for split_key in rechunk_splits:
        split_value = rechunk_merge_graph[split_key]
        _, array_vals, slices = split_value
        target_name, s1, s2, s3 = array_vals
        slices_list.append((s1, s2, s3))
    
    for merge_key in rechunk_merges:
        merge_value = rechunk_merge_graph[merge_key]
        _, concat_list = merge_value
        l = concat_list
        while not isinstance(l[0][0], tuple):
            l = l[0]
        for block in l:
            for tupl in block:
                if 'array' in tupl[0]:
                    target_name, s1, s2, s3 = tupl
                    slices_list.append((s1, s2, s3))

    return target_name, slices_list


def get_slices_from_getitem_dict(getitem_dict):
    """ 
    -assumes that the getitem dict only take data from a same proxy array
    -assumes that the proxy array parts are collected entirely so does not consider the slices
    (assumes slices = ((None,None,None),(None,None,None),(None,None,None)))
    this functions finds the proxy array and collect the proxy array parts 
    """
    slices_list = list()
    target_name = None
    for key in list(getitem_dict.keys()): 
        getitem_task = getitem_dict[key]
        array_and_slice = getitem_task[1]
        _slice = array_and_slice[1:]
        slices_list.append(tuple(_slice))
        if not target_name:
            target_name = array_and_slice[0]
    return target_name, slices_list


## utilities (to print and/or visualize)


def get_rechunk_keys_lists_from_dask_array(graph, printer=False):
    keys = list(graph.keys())
    others = list()
    proxy_array_keys = list()
    rechunk_merge_keys = list()
    for key in keys:
        if "array" in key:
            proxy_array_keys.append(key)
        elif 'rechunk-merge' in key:
            rechunk_merge_keys.append(key)
        else:
            others.append(key)

    if len(rechunk_merge_keys) == 0:
        raise ValueError("bad function used")

    for rechunk_merge_key in rechunk_merge_keys:
        rechunk_graph = graph[rechunk_merge_key]
        keys = list(rechunk_graph.keys())
        rechunk_keys = list()
        for k in keys:
            if "rechunk-split" in k[0]:
                rechunk_keys.append((k, rechunk_merge_key))
        
    if printer:
        print(rechunk_keys)
        print(proxy_array_keys)
        print(others)
    return rechunk_keys, rechunk_graph, proxy_array_keys, others


def get_keys_lists_from_dask_array(graph, printer=False):
    keys = list(graph.keys())
    getitem_keys = list()
    proxy_array_keys = list()
    actions = list()

    for k in keys:
        if "array" in k:
            proxy_array_keys.append(k)
        elif "getitem" in k:
            getitem_keys.append(k)
        else:
            actions.append(k)

    if printer:
        print(getitem_keys)
        print(proxy_array_keys)
        print(actions)
    return getitem_keys, proxy_array_keys, actions


def print_array_parts(graph, display_real_slices=False):
    """
    if not display_real_slices: print range for x, range for y, range for z
    if display_real_slices: print the slices used in the program without reformating before printing
    """
    getitem_keys, _, _ = get_keys_lists_from_dask_array(graph)

    getitem_supertasks = dict()
    for key in getitem_keys:
        getitem_graph = graph[key]
        getitem_parts = list(getitem_graph.keys())
        getitem_content = dict()
        for getitem_part in getitem_parts:
            value = getitem_graph[getitem_part]
            proxy_array_name = value[1][0]
            proxy_array_part = tuple(value[1][1:])
            slice_from_array_part = value[2]

            if display_real_slices:
                part_x = [s.start for s in slice_from_array_part]
                part_y = [s.stop for s in slice_from_array_part]
                part_z = [s.step for s in slice_from_array_part]

                slice_from_array_part = (part_x, part_y, part_z)

            if proxy_array_name in list(getitem_content.keys()):
                getitem_content[proxy_array_name].append((proxy_array_part, slice_from_array_part))
            else:
                getitem_content[proxy_array_name]= [(proxy_array_part, slice_from_array_part)]
        getitem_supertasks[key] = getitem_content

    for task_key in list(getitem_supertasks.keys()):
        print(task_key)
        task = getitem_supertasks[task_key]
        for array in list(task.keys()):
            print("\t", array)
            for e in task[array]:
                print("\t\t", e)
            print("\n")
        print("\n")
    return getitem_supertasks


def print_dask_graph_first_layer(dask_graph):
    """ print the first layer of the dask graph including keys and associated values in a clear way
    """
    print("dask_graph")
    for key, val in dask_graph.items():
        if not 'array-' in key:
            print("key", key, "\n")
            print("val", val, "\n\n")