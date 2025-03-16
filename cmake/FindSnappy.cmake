# Locate snappy library
# This module defines
#  SNAPPY_FOUND, if false, do not try to link with snappy
#  LIBSNAPPY, Library path and libs
#  SNAPPY_INCLUDE_DIR, where to find the ICU headers

find_path(SNAPPY_INCLUDE_DIR snappy.h
  HINTS
  ENV SNAPPY_DIR
  PATH_SUFFIXES include
  PATHS
  ~/Library/Frameworks
  /Library/Frameworks
  /usr/local
  /opt/local
  /opt/csw
  /opt/snappy
  /opt)

find_path(SNAPPY_LIBRARIES
  NAMES snappy
  HINTS
  ENV SNAPPY_DIR
  PATHS
  ~/Library/Frameworks
  /Library/Frameworks
  /usr/local
  /opt/local
  /opt/csw
  /opt/snappy
  /opt)

find_path(SNAPPY_STATIC_LIBRARIES
  NAMES libsnappy.a
  HINTS
  ENV SNAPPY_DIR
  PATHS
  ~/Library/Frameworks
  /Library/Frameworks
  /usr/local
  /opt/local
  /opt/csw
  /opt/snappy
  /opt)

if(SNAPPY_LIBRARIES AND SNAPPY_INCLUDE_DIR)
  include_directories(AFTER ${SNAPPY_INCLUDE_DIR})
  message(STATUS "Found snappy in ${SNAPPY_INCLUDE_DIR} : ${SNAPPY_LIBRARIES}")
  set(SNAPPY_FOUND ON)

  mark_as_advanced(SNAPPY_INCLUDE_DIR SNAPPY_LIBRARIES)
else()
  message(STATUS "Snappy : NOT Found")
  set(SNAPPY_FOUND OFF)

endif(SNAPPY_LIBRARIES AND SNAPPY_INCLUDE_DIR)
