add_executable(mini-hadoop-tests
  common/error_test.cpp
  io/buffer_test.cpp
  io/writable_types_test.cpp
  io/md5_test.cpp
  ipc/ipc_test.cpp
  ndfs/inode_tree_test.cpp
  ndfs/fs_namesystem_test.cpp
  ndfs/block_storage_test.cpp
  ndfs/ndfs_integration_test.cpp
  mapred/mapred_test.cpp
  mapred/dist_mapred_test.cpp
  full_chain_test.cpp
  stress_test.cpp
)

target_link_libraries(mini-hadoop-tests PRIVATE
  GTest::gtest
  GTest::gtest_main
  spdlog::spdlog
  mini-hadoop-io
  mini-hadoop-ipc
  mini-hadoop-ndfs
  mini-hadoop-mapred
)

target_include_directories(mini-hadoop-tests PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

include(GoogleTest)
gtest_discover_tests(mini-hadoop-tests)
