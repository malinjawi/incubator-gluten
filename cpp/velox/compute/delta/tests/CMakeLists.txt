add_executable(velox_roaring_bitmap_array_test RoaringBitmapArrayTest.cpp)

target_link_libraries(
  velox_roaring_bitmap_array_test velox roaring GTest::gtest GTest::gtest_main)

add_test(
  NAME velox_roaring_bitmap_array_test
  COMMAND velox_roaring_bitmap_array_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
