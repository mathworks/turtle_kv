#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the TurtleKV Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

import io, os, platform, shlex, subprocess, sys

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.scm import Version


class TurtleKvRecipe(ConanFile):
    name = "turtle_kv"

    python_requires = "cor_recipe_utils/0.19.1"
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

    options = {
        "with_keyvcr": [True, False],
        "use_bloom_filter": [True, False],
        "use_quotient_filter": [True, False],
        "enable_leaf_filters": [True, False],
        "enable_metrics": [True, False],
        "profile_updates": [True, False],
        "profile_queries": [True, False],
    }

    default_options = {
        "with_keyvcr": False,
        "use_bloom_filter": False,
        "use_quotient_filter": True,
        "enable_leaf_filters": True,
        "enable_metrics": True,
        "profile_updates": True,
        "profile_queries": True,
    }

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
        self.requires("artc/[>=0.0.1 <1]")
        self.requires("batteries/[>=0.70.2 <1]", **VISIBLE, **OVERRIDE)
        self.requires("boost/1.88.0", **VISIBLE, **OVERRIDE)
        self.requires("glog/0.7.1", **VISIBLE)
        self.requires("llfs/[>=0.44.0 <1]", **VISIBLE)
        self.requires("pcg-cpp/cci.20220409", **VISIBLE)
        self.requires("yaml-cpp/[>=0.9.0 <1]")
        self.requires("zlib/1.3.1", **OVERRIDE)

        # boost/1.88.0 and ninja/1.13.2 depend (exactly) on libbacktrace/cci.20210118
        #
        self.requires("libbacktrace/[>=cci.20240730]", **OVERRIDE)

        if platform.system() == "Linux":
            if self.options.with_keyvcr:
                self.requires("keyvcr/[>=0.2.2 <1]", **VISIBLE)
            if self.options.use_quotient_filter:
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
        self.cor.set_version_from_git_tags(self)

    def layout(self):
        self.cor.layout_cmake_unified_src(self)
        self.cpp.build.libdirs += ['src']
        self.cpp.build.libs += ['turtle_kv']

    def generate(self):
        self.cor.generate_cmake_default(self)

    def build(self):
        self.cor.build_cmake_default(self)

    def package(self):
        self.cor.package_cmake_lib_default(self)

    def package_info(self):
        self.cor.package_info_lib_default(self)

    def package_id(self):
        self.cor.package_id_lib_default(self)

    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def validate_build(self):
        if self.settings.compiler == "gcc":
            out_capture = io.StringIO()
            from_conf = self.conf.get('tools.build:compiler_executables')
            cc_name = (
                self.buildenv.vars(self).get('CC') or
                self.buildenv.vars(self).get('CXX') or
                os.getenv('CC') or
                os.getenv('CXX') or
                (from_conf and from_conf.get('cpp')) or
                'gcc'
            )
            self.run(shlex.join([cc_name, '-dumpversion']), stdout=out_capture, shell=True)
            actual_compiler_version = Version(out_capture.getvalue().strip())
            profile_compiler_version = Version(str(self.settings.compiler.version))

            if profile_compiler_version.major != actual_compiler_version.major:
                raise ConanInvalidConfiguration(f"Compiler version mismatch: actual={actual_compiler_version}"
                                                f", expected={profile_compiler_version}")

    #+++++++++++-+-+--+----- --- -- -  -  -   -


