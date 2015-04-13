# This module defines
#  RECORD_SERVICE_INCLUDE_DIR, directory containing headers
#  RECORD_SERVICE_LIBS, directory containing record service libraries
#  RECORD_SERVICE_STATIC_LIB, path to libRecordServiceThrift.a
#  RECORD_SERVICE_FOUND, whether RecordService has been found

set(RECORD_SERVICE_SEARCH_HEADER_PATHS
  $ENV{RECORD_SERVICE_HOME}/cpp/generated-sources/
)

set(RECORD_SERVICE_SEARCH_LIB_PATH
  $ENV{RECORD_SERVICE_HOME}/cpp/build/release/thrift
)

find_path(RECORD_SERVICE_INCLUDE_DIR gen-cpp/RecordServicePlanner.h PATHS
  ${RECORD_SERVICE_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(RECORD_SERVICE_LIB_PATH NAMES RecordServiceThrift 
    PATHS ${RECORD_SERVICE_SEARCH_LIB_PATH})

if (RECORD_SERVICE_LIB_PATH)
  set(RECORD_SERVICE_FOUND TRUE)
  set(RECORD_SERVICE_LIBS ${RECORD_SERVICE_SEARCH_LIB_PATH})
  set(RECORD_SERVICE_STATIC_LIB ${RECORD_SERVICE_SEARCH_LIB_PATH}/libRecordServiceThrift.a)
else ()
  set(RECORD_SERVICE_FOUND FALSE)
endif ()

if (RECORD_SERVICE_FOUND)
  message(STATUS "RecordService found in ${RECORD_SERVICE_SEARCH_LIB_PATH}")
else ()
  message(STATUS "RecordService includes and libraries NOT found. "
    "Looked for headers in ${RECORD_SERVICE_SEARCH_HEADER_PATHS}, "
    "and for libs in ${RECORD_SERVICE_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  RECORD_SERVICE_INCLUDE_DIR
  RECORD_SERVICE_LIBS
  RECORD_SERVICE_STATIC_LIB
)
