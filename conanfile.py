from conan import ConanFile
from conan.errors import ConanInvalidConfiguration

import io, os, platform, shlex, subprocess, sys


class TurtleKvRecipe(ConanFile):
    name = "turtle_kv"

    python_requires = "cor_recipe_utils/0.18.2"
    python_requires_extend = "cor_recipe_utils.ConanFileBase"

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
    license = "Apache 2.0"

    author = "MathWorks"

    url = "https://github.com/mathworks/turtle_kv"

    description = (
        "A high-performance embedded key-value database supporting dynamic "
        "memory-based performance tuning"
    )

    topics = ("database", "mathworks", "key-value")
    #
    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def requirements(self):
        VISIBLE = self.cor.VISIBLE
        OVERRIDE = {
            "force": True,
        }

        self.requires("abseil/20250127.0", **VISIBLE, **OVERRIDE)
        self.requires("batteries/[>=0.61.0 <1]", **VISIBLE, **OVERRIDE)
        self.requires("boost/1.88.0", **VISIBLE, **OVERRIDE)
        self.requires("glog/0.7.1", **VISIBLE)
        self.requires("llfs/[>=0.43.3 <1]", **VISIBLE)
        self.requires("pcg-cpp/cci.20220409", **VISIBLE)
        self.requires("zlib/1.3.1", **OVERRIDE)

        # boost/1.88.0 and ninja/1.13.2 depend (exactly) on libbacktrace/cci.20210118
        #
        self.requires("libbacktrace/[>=cci.20240730]", **OVERRIDE)

        if platform.system() == "Linux":
            self.requires("keyvcr/[>=0.2.1 <1]", **VISIBLE)
            self.requires("vqf/[>=0.2.5 <1]", **VISIBLE)
            self.requires("libfuse/[>=3.16.2 <4]", **VISIBLE)
            self.requires("libunwind/[>=1.8.1 <2]", **VISIBLE, **OVERRIDE)
            self.requires("liburing/[>=2.11 <3]", **VISIBLE)

    def build_requirements(self):
        self.tool_requires("cmake/[>=3.20.0 <4]")
        self.tool_requires("ninja/[>=1.10.2 <2]")
        self.test_requires("gtest/[>=1.16.0 <2]")

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

    def validate_build(self):
        if self.settings.compiler == "gcc":
            out_capture = io.StringIO()
            cc_name = (
                self.buildenv.vars(self).get('CC') or
                self.buildenv.vars(self).get('CXX') or
                os.getenv('CC') or
                os.getenv('CXX') or
                'gcc'
            )
            self.run(shlex.join([cc_name, '-dumpversion']), stdout=out_capture, shell=True)
            actual_compiler_version = out_capture.getvalue().strip()
            profile_compiler_version = str(self.settings.compiler.version)
            if profile_compiler_version != actual_compiler_version:
                raise ConanInvalidConfiguration(f"Compiler version mismatch: actual={actual_compiler_version}"
                                                f", expected={profile_compiler_version}")

