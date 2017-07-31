__author__ = 'Marco'

class responseCode:

    # return types
    DB_ERROR = -1
    ELEM_TYPE_ERROR = 0
    ELEM_CREATE_SUCCESS = 1
    ELEM_ALREADY_EXIST = 2
    ELEM_NOT_EXIST = 3
    ELEM_UPDATE_SUCCESS = 4
    ELEM_GET_SUCCESS = 5
    ELEM_INCR_SUCCESS = 6
    ELEM_DECR_SUCCESS = 7
    ELEM_SEARCH_SUCCESS = 8
    DB_SAVE_ERROR = 9
    DB_SAVE_SUCCESS = 10
    ELEM_IS_LOCKED = 11
    DB_SAVE_LOCKED = 12
    ELEM_DELETE_SUCCESS = 13
    DB_EXISTED = 14
    DB_CREATE_SUCCESS = 15
    DB_GET_SUCCESS = 16
    LIST_CREATE_SUCCESS = 17
    LIST_ALREADY_EXIST = 18
    LIST_IS_LOCKED = 19
    LIST_INSERT_SUCCESS = 20
    LIST_NOT_CONTAIN_VALUE = 21
    LIST_REMOVE_SUCCESS = 22
    LIST_NOT_EXIST = 23
    LIST_SEARCH_SUCCESS = 24
    LIST_GET_SUCCESS = 25
    LIST_DELETE_SUCCESS = 26
    DB_DELETE_SUCCESS = 27
    DB_NOT_EXIST = 28
    HASH_CREATE_SUCCESS = 29
    HASH_EXISTED = 30
    HASH_NOT_EXISTED = 31
    HASH_GET_SUCCESS = 32
    HASH_IS_LOCKED = 33
    HASH_INSERT_SUCCESS = 34
    HASH_KEY_EXIST = 35
    HASH_KEY_NOT_EXIST = 36
    HASH_DELETE_SUCCESS = 37
    HASH_SEARCH_SUCCESS = 38
    HASH_REMOVE_SUCCESS = 39
    HASH_CLEAR_SUCCESS = 40
    HASH_REPLACE_SUCCESS = 41
    LIST_CLEAR_SUCCESS = 42
    MERGE_RESULT_EXIST = 43
    LIST_MERGE_SUCCESS = 44
    HASH_MERGE_SUCCESS = 45
    ELEM_TTL_SET_SUCCESS = 46
    ELEM_TTL_CLEAR_SUCCESS = 47
    ELEM_EXPIRED = 48
    LIST_TTL_SET_SUCCESS = 49
    LIST_TTL_CLEAR_SUCCESS = 50
    LIST_EXPIRED = 51
    HASH_TTL_SET_SUCCESS = 52
    HASH_TTL_CLEAR_SUCCESS = 53
    HASH_EXPIRED = 54
    SET_CREATE_SUCCESS = 55
    SET_ALREADY_EXIST = 56
    SET_GET_SUCCESS = 57
    SET_NOT_EXIST = 58
    SET_IS_LOCKED = 59
    SET_VALUE_ALREADY_EXIST = 60
    SET_INSERT_SUCCESS = 61
    SET_VALUE_NOT_EXIST = 62
    SET_REMOVE_SUCCESS = 63
    SET_CLEAR_SUCCESS = 64