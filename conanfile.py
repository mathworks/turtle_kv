from conan import ConanFile
import os, sys, platform


class TurtleKvRecipe(ConanFile):
    name = "turtle_kv"

    python_requires = "cor_recipe_utils/0.10.0"
    python_requires_extend = "cor_recipe_utils.ConanFileBase"

    tool_requires = [
        "cmake/[>=3.20.0]",
        "ninja/1.12.1",
    ]

    settings = "os", "compiler", "build_type", "arch"

    exports_sources = [
        "CMakeLists.txt",
        "**/CMakeLists.txt",
        "src/*.h",
        "src/*.hpp",
        "src/**/*.h",
        "src/**/*.hpp",
        "src/*.ipp",
        "src/**/*.ipp",
        "src/*.cpp",
        "src/**/*.cpp",
        "bench/*.cpp",
        "bench/*.hpp",
        "bench/*.ipp",
        "bench/**/*.cpp",
        "bench/**/*.hpp",
        "bench/**/*.ipp",
    ]

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    # Optional metadata
    #
    license = "TODO"

    author = "TODO"

    url = "TODO"

    description = "TODO"

    topics = ("TODO",)
    #
    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def requirements(self):
        VISIBLE = self.cor.VISIBLE
        OVERRIDE = self.cor.OVERRIDE

        self.requires("abseil/[>=20250127.0]", **VISIBLE, **OVERRIDE)
        #self.requires("batteries/[>=0.59.0 <1]", **VISIBLE, **OVERRIDE)
        self.requires("batteries/0.59.3-devel", **VISIBLE)
        self.requires("boost/[>=1.85.0 <2]", **VISIBLE, **OVERRIDE)
        self.requires("glog/[>=0.7.0 <1]", **VISIBLE)
        self.requires("gperftools/[>=2.16 <3]", **VISIBLE)
        #self.requires("jemalloc/5.3.0", **VISIBLE, **OVERRIDE)
        #self.requires("llfs/[>=0.41.18 <1]", **VISIBLE)
        self.requires("llfs/0.41.20.dev14", **VISIBLE)
        self.requires("pcg-cpp/[>=cci.20220409]", **VISIBLE)
        self.requires("vqf/[>=0.2.4 <1]", **VISIBLE)
        self.requires("zlib/[>=1.3 <2]", **OVERRIDE)

        if platform.system() == "Linux":
            self.requires("libfuse/[>=3.16.2 <4]", **VISIBLE)
            self.requires("libunwind/[>=1.8.0 <2]", **VISIBLE, **OVERRIDE)
            self.requires("liburing/[>=2.4 <3]", **VISIBLE)

        self.test_requires("gtest/[>=1.14.0 <2]")

    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["boost"].without_test = True
        self.options["batteries"].with_glog = True
        self.options["batteries"].header_only = False

    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def set_version(self):
        return self.cor.set_version_from_git_tags(self)

    def layout(self):
        return self.cor.layout_cmake_unified_src(self)

    def generate(self):
        return self.cor.generate_cmake_default(self)

    def build(self):
        return self.cor.build_cmake_default(self)

    def package(self):
        return self.cor.package_cmake_lib_default(self)

    def package_info(self):
        return self.cor.package_info_lib_default(self)

    def package_id(self):
        return self.cor.package_id_lib_default(self)

    #+++++++++++-+-+--+----- --- -- -  -  -   -
