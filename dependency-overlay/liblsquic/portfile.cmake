if(VCPKG_TARGET_IS_WINDOWS)
  # The lib uses CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS, at least until
  # https://github.com/litespeedtech/lsquic/pull/371 or similar is merged
  vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

vcpkg_from_github(OUT_SOURCE_PATH SOURCE_PATH
    REPO litespeedtech/lsquic
    REF v4.6.0
    SHA512 7de5d13e6252c2451586ecbe5a9c31f8cd2c22506d7ebbdca208944048e3e3d970b94bf2d9e66e1602a55cae830446eb0e3fe1fb309a5c59ec2ae711b9831ba9
    HEAD_REF master
)

# Submodules
vcpkg_from_github(OUT_SOURCE_PATH LSQPACK_SOURCE_PATH
    REPO litespeedtech/ls-qpack
    REF v2.6.2
    HEAD_REF master
    SHA512 9b38ba1b4b12d921385a285e8c833a0ae9cdcc153cff4f1857f88ceb82174304decb5fccbdf9267d08a21c5a26c71fdd884dcacd12afd19256a347a8306b9b90
)
if(NOT EXISTS "${SOURCE_PATH}/src/ls-hpack/CMakeLists.txt")
    file(REMOVE_RECURSE "${SOURCE_PATH}/src/liblsquic/ls-qpack")
    file(RENAME "${LSQPACK_SOURCE_PATH}" "${SOURCE_PATH}/src/liblsquic/ls-qpack")
endif()

vcpkg_from_github(OUT_SOURCE_PATH LSHPACK_SOURCE_PATH
    REPO litespeedtech/ls-hpack
    REF v2.3.2
    HEAD_REF master
    SHA512 45d6c8296e8eee511e6a083f89460d5333fc9a49bc078dac55fdec6c46db199de9f150379f02e054571f954a5e3c79af3864dbc53dc57d10a8d2ed26a92d4278
)
if(NOT EXISTS "${SOURCE_PATH}/src/lshpack/CMakeLists.txt")
    file(REMOVE_RECURSE "${SOURCE_PATH}/src/lshpack")
    file(RENAME "${LSHPACK_SOURCE_PATH}" "${SOURCE_PATH}/src/lshpack")
endif()

# Configuration
vcpkg_find_acquire_program(PERL)

string(COMPARE EQUAL "${VCPKG_LIBRARY_LINKAGE}" "dynamic" LSQUIC_SHARED_LIB)

vcpkg_cmake_configure(
  SOURCE_PATH "${SOURCE_PATH}"
  OPTIONS
    "-DPERL=${PERL}"
    "-DPERL_EXECUTABLE=${PERL}"
    "-DLSQUIC_SHARED_LIB=${LSQUIC_SHARED_LIB}"
    "-DBORINGSSL_INCLUDE=${CURRENT_INSTALLED_DIR}/include"
    -DLSQUIC_BIN=OFF
    -DLSQUIC_TESTS=OFF
  OPTIONS_RELEASE
    "-DBORINGSSL_LIB=${CURRENT_INSTALLED_DIR}/lib"
  OPTIONS_DEBUG
    "-DBORINGSSL_LIB=${CURRENT_INSTALLED_DIR}/debug/lib"
    -DLSQUIC_DEVEL=ON
)

vcpkg_cmake_install()
if(VCPKG_TARGET_IS_WINDOWS)
  # Upstream removed installation of this header after merging changes
  file(INSTALL "${SOURCE_PATH}/wincompat/vc_compat.h" DESTINATION "${CURRENT_INSTALLED_DIR}/include/lsquic")
endif()

vcpkg_cmake_config_fixup(PACKAGE_NAME lsquic)

# Concatenate license files and install
vcpkg_install_copyright(FILE_LIST 
  "${SOURCE_PATH}/LICENSE" 
  "${SOURCE_PATH}/LICENSE.chrome"
)

# Remove duplicated include directory
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

