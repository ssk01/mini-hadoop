add_library(mini-hadoop-io INTERFACE
  io/buffer.h
  io/writable.h
  io/writable_types.h
  io/md5.h
  io/sequence_file.h
)

target_link_libraries(mini-hadoop-io INTERFACE)
if(APPLE)
  target_link_libraries(mini-hadoop-io INTERFACE "-framework Security")
else()
  target_link_libraries(mini-hadoop-io INTERFACE OpenSSL::Crypto)
endif()

add_library(mini-hadoop-ipc INTERFACE
  ipc/server.h
  ipc/client.h
)

target_link_libraries(mini-hadoop-ipc INTERFACE mini-hadoop-io spdlog::spdlog)

add_library(mini-hadoop-ndfs INTERFACE
  ndfs/block.h
  ndfs/datanode_info.h
  ndfs/inode_tree.h
  ndfs/fs_namesystem.h
  ndfs/name_node.h
  ndfs/block_storage.h
  ndfs/data_node.h
  ndfs/dfs_client.h
)

target_link_libraries(mini-hadoop-ndfs INTERFACE mini-hadoop-ipc)

add_library(mini-hadoop-mapred INTERFACE
  mapred/api/mapper.h
  mapred/api/input_format.h
  mapred/job_tracker.h
  mapred/dist_job_tracker.h
  mapred/task_tracker.h
  mapred/examples/wordcount.h
)

target_link_libraries(mini-hadoop-mapred INTERFACE mini-hadoop-io)

add_executable(mini-hadoop
  main.cpp
  common/error.h
)

target_link_libraries(mini-hadoop PRIVATE
  spdlog::spdlog
  tomlplusplus::tomlplusplus
  mini-hadoop-io
  mini-hadoop-ipc
  mini-hadoop-ndfs
  mini-hadoop-mapred
)

target_include_directories(mini-hadoop PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
