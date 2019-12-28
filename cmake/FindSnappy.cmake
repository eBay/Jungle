# Locate snappy library
# This module defines
#  SNAPPY_FOUND, if false, do not try to link with snappy
#  LIBSNAPPY, Library path and libs
#  SNAPPY_INCLUDE_DIR, where to find the ICU headers

FIND_PATH(SNAPPY_INCLUDE_DIR snappy.h
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

FIND_LIBRARY(SNAPPY_LIBRARIES
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

FIND_LIBRARY(SNAPPY_STATIC_LIBRARIES
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

IF (SNAPPY_LIBRARIES AND SNAPPY_INCLUDE_DIR )
  include_directories(AFTER ${SNAPPY_INCLUDE_DIR})
  MESSAGE(STATUS "Found snappy in ${SNAPPY_INCLUDE_DIR} : ${SNAPPY_LIBRARIES}")
  SET(SNAPPY_FOUND ON)

   MARK_AS_ADVANCED(SNAPPY_INCLUDE_DIR SNAPPY_LIBRARIES)
ELSE (SNAPPY_LIBRARIES AND SNAPPY_INCLUDE_DIR )
  MESSAGE(STATUS "Snappy : NOT Found")
  SET(SNAPPY_FOUND OFF)

ENDIF (SNAPPY_LIBRARIES AND SNAPPY_INCLUDE_DIR )
