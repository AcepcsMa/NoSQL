__author__ = 'Ma Haoxiang'

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
    SET_SEARCH_SUCCESS = 65
    SET_UNION_SUCCESS = 66
    SET_INTERSECT_SUCCESS = 67
    SET_DIFF_SUCCESS = 68
    SET_REPLACE_SUCCESS = 69
    SET_TTL_SET_SUCCESS = 70
    SET_EXPIRED = 71
    SET_TTL_CLEAR_SUCCESS = 72
    TTL_NO_RECORD = 73
    TTL_SHOW_SUCCESS = 74
    TTL_EXPIRED = 75
    DATA_TYPE_ERROR = 76
    SET_DELETE_SUCCESS = 77
    KEY_NAME_INVALID = 78
    ELEM_NOT_SET_TTL = 79
    LIST_NOT_SET_TTL = 80
    HASH_NOT_SET_TTL = 81
    SET_NOT_SET_TTL = 82
    ZSET_CREATE_SUCCESS = 83
    ZSET_ALREADY_EXIST = 84
    ZSET_GET_SUCCESS = 85
    ZSET_EXPIRED = 86
    ZSET_NOT_EXIST = 87
    ZSET_IS_LOCKED = 88
    ZSET_INSERT_SUCCESS = 89
    ZSET_VALUE_ALREADY_EXIST = 90
    ZSET_REMOVE_SUCCESS = 91
    ZSET_NOT_CONTAIN_VALUE = 92
    ZSET_CLEAR_SUCCESS = 93
    ZSET_DELETE_SUCCESS = 94
    ZSET_SEARCH_SUCCESS = 95
    ZSET_FIND_MIN_SUCCESS = 96
    ZSET_FIND_MAX_SUCCESS = 96
    ZSET_IS_EMPTY = 97
    ZSET_GET_SCORE_SUCCESS = 98
    ZSET_SCORE_RANGE_ERROR = 99
    ZSET_GET_VALUES_SUCCESS = 100
    ZSET_GET_RANK_SUCCESS = 101
    ZSET_REMOVE_BY_SCORE_SUCCESS = 102
    ZSET_TTL_SET_SUCCESS = 103
    ZSET_NOT_SET_TTL = 104
    ZSET_TTL_CLEAR_SUCCESS = 105
    GET_SIZE_SUCCESS = 106
    LIST_RANGE_ERROR = 107
    ELEM_TYPE = 108
    LIST_TYPE = 109
    HASH_TYPE = 110
    SET_TYPE = 111
    ZSET_TYPE = 112
    HASH_KEYSET_GET_SUCCESS = 113
    HASH_VALUES_GET_SUCCESS = 114
    HASH_INCR_SUCCESS = 115
    HASH_DECR_SUCCESS = 116
    LIST_LENGTH_TOO_SHORT = 117
    INVALID_NUMBER = 118
    SET_LENGTH_TOO_SHORT = 119
    SAVE_INTERVAL_CHANGE_SUCCESS = 120

    detail = {
        HASH_CREATE_SUCCESS:"Hash Create Success",
        HASH_EXISTED:"Hash Already Exists",
        KEY_NAME_INVALID:"Key Name Is Invalid",
        HASH_IS_LOCKED:"Hash Is Locked",
        HASH_INSERT_SUCCESS:"Hash Insert Success",
        HASH_DELETE_SUCCESS:"Hash Delete Success",
        HASH_REMOVE_SUCCESS:"Hash Remove Success",
        HASH_CLEAR_SUCCESS:"Hash Clear Success",
        HASH_REPLACE_SUCCESS:"Hash Replace Success",
        HASH_MERGE_SUCCESS:"Hash Merge Success",
        HASH_TTL_SET_SUCCESS:"Hash TTL Set Success",
        HASH_TTL_CLEAR_SUCCESS:"Hash TTL Clear Success",
        TTL_NO_RECORD:"TTL No Record",
        TTL_EXPIRED:"TTL Expired",
        TTL_SHOW_SUCCESS:"TTL Show Success",
        ELEM_CREATE_SUCCESS:"Element Create Success",
        ELEM_IS_LOCKED:"Element Is Locked",
        ELEM_UPDATE_SUCCESS:"Element Update Success",
        ELEM_INCR_SUCCESS:"Element Increase Success",
        ELEM_DECR_SUCCESS:"Element Decrease Success",
        ELEM_DELETE_SUCCESS:"Element Delete Success",
        ELEM_TTL_SET_SUCCESS:"Element TTL Set Success",
        ELEM_TTL_CLEAR_SUCCESS:"Element TTL Clear Success",
        ELEM_NOT_SET_TTL:"Element Is Not Set TTL",
        LIST_CREATE_SUCCESS:"List Create Success",
        LIST_IS_LOCKED:"List Is Locked",
        LIST_INSERT_SUCCESS:"List Insert Success",
        LIST_DELETE_SUCCESS:"List Delete Success",
        LIST_NOT_CONTAIN_VALUE:"List Does Not Contain This Value",
        LIST_REMOVE_SUCCESS:"List Remove Value Success",
        LIST_CLEAR_SUCCESS:"List Clear Success",
        LIST_TTL_SET_SUCCESS:"List TTL Set Success",
        LIST_TTL_CLEAR_SUCCESS:"List TTL Clear Success",
        LIST_NOT_SET_TTL:"List Is Not Set TTL",
        SET_CREATE_SUCCESS:"Set Create Success",
        SET_IS_LOCKED:"Set Is Locked",
        SET_VALUE_ALREADY_EXIST:"Set Value Already Exists",
        SET_INSERT_SUCCESS:"Set Insert Success",
        SET_VALUE_NOT_EXIST:"Set Value Does Not Exist",
        SET_REMOVE_SUCCESS:"Set Remove Success",
        SET_CLEAR_SUCCESS:"Set Clear Success",
        SET_DELETE_SUCCESS:"Set Delete Success",
        SET_REPLACE_SUCCESS:"Set Replace Success",
        SET_TTL_SET_SUCCESS:"Set TTL Set Success",
        SET_TTL_CLEAR_SUCCESS:"Set TTL Clear Success",
        LIST_MERGE_SUCCESS:"Lists Merge Success",
        HASH_NOT_SET_TTL:"Hash Is Not Set TTL",
        SET_UNION_SUCCESS:"Set Union Success",
        SET_INTERSECT_SUCCESS:"Set Intersect Success",
        SET_DIFF_SUCCESS:"Set Diff Success",
        SET_NOT_SET_TTL:"Set Is Not Set TTL",
        ZSET_CREATE_SUCCESS:"ZSet Create Success",
        ZSET_IS_LOCKED:"ZSet Is Locked",
        ZSET_INSERT_SUCCESS:"ZSet Insert Success",
        ZSET_VALUE_ALREADY_EXIST:"ZSet Value Already Exists",
        ZSET_REMOVE_SUCCESS:"ZSet Remove Value Success",
        ZSET_NOT_CONTAIN_VALUE:"ZSet Does Not Contain This Value",
        ZSET_CLEAR_SUCCESS:"ZSet Clear Success",
        ZSET_DELETE_SUCCESS:"ZSet Delete Success",
        ZSET_SEARCH_SUCCESS:"ZSet Search Success",
        ZSET_REMOVE_BY_SCORE_SUCCESS:"ZSet Remove By Score Success",
        ZSET_TTL_SET_SUCCESS:"ZSet TTL Set Success",
        ZSET_NOT_SET_TTL:"ZSet Is Not Set TTL",
        ZSET_TTL_CLEAR_SUCCESS:"ZSet TTL Clear Success",
        GET_SIZE_SUCCESS:"Get Size Success",
        ELEM_TYPE:"Element",
        LIST_TYPE:"List",
        HASH_TYPE:"Hash",
        SET_TYPE:"Set",
        ZSET_TYPE:"ZSet",
        HASH_INCR_SUCCESS:"Hash Value Increase Success",
        ELEM_TYPE_ERROR:"Element Type Error",
        HASH_DECR_SUCCESS:"Hash Value Decrease Success",
        LIST_LENGTH_TOO_SHORT:"List Length Is Too Short",
        LIST_GET_SUCCESS:"List Value Get Success",
        SET_LENGTH_TOO_SHORT:"Set Length Is Too Short",
        SET_GET_SUCCESS:"Set Value Get Success"
    }